// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	alicreds "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
)

const (
	envRoleArn         = "ALIBABA_CLOUD_ROLE_ARN"
	envOidcProviderArn = "ALIBABA_CLOUD_OIDC_PROVIDER_ARN"
	envOidcTokenFile   = "ALIBABA_CLOUD_OIDC_TOKEN_FILE"

	defaultSTSEndpoint     = "sts.aliyuncs.com"
	defaultDurationSeconds = 3600 * 12

	// http query
	queryVersion = "2015-04-01"
	queryAction  = "AssumeRoleWithOIDC"
	qeuryFormat  = "JSON"

	oidcProviderName = "oidc_role_arn"
	contentType      = "application/x-www-form-urlencoded"
)

type sessionCredentials struct {
	AccessKeyId     string
	AccessKeySecret string
	SecurityToken   string
	Expiration      string
}

// TODO: unused
type httpOptions struct {
	proxy string
	// Connection timeout, in milliseconds.
	connectTimeout int
	// Read timeout, in milliseconds.
	readTimeout int
}

type ossOIDCCredentialsProvider struct {
	oidcProviderARN   string
	oidcTokenFilePath string
	roleARN           string
	roleSessionName   string
	durationSeconds   int
	policy            string // TODO: unused
	// for sts endpoint
	stsEndpoint string

	lastUpdateTimestamp int64
	expirationTimestamp int64
	sessionCredentials  *sessionCredentials
	// for http options
	httpOptions *httpOptions
}

func newOssOIDCCredentialsProvider(duration time.Duration) (*ossOIDCCredentialsProvider, error) {
	oidcTokenFilePath := os.Getenv(envOidcTokenFile)
	if len(oidcTokenFilePath) == 0 {
		return nil, errors.New("the OIDCTokenFilePath is empty")
	}
	oidcProviderARN := os.Getenv(envOidcProviderArn)
	if len(oidcProviderARN) == 0 {
		return nil, errors.New("the oidcProviderARN is empty")
	}
	roleARN := os.Getenv(envRoleArn)
	if len(roleARN) == 0 {
		return nil, errors.New("the roleARN is empty")
	}

	durationSeconds := defaultDurationSeconds
	if duration != 0 {
		durationSeconds = int(duration.Seconds())
	}
	if durationSeconds < 900 {
		return nil, errors.New("the Assume Role session duration of the Oss should be in the range of 15min - max duration seconds")
	}

	roleSessionName := "credentials-go-" + strconv.FormatInt(time.Now().UnixNano()/1000, 10)

	provider := &ossOIDCCredentialsProvider{
		oidcProviderARN:   oidcProviderARN,
		oidcTokenFilePath: oidcTokenFilePath,
		roleARN:           roleARN,
		roleSessionName:   roleSessionName,
		durationSeconds:   durationSeconds,
		stsEndpoint:       defaultSTSEndpoint,
	}
	return provider, nil
}

type assumedRoleUser struct {
}

type ossCredentials struct {
	SecurityToken   *string `json:"SecurityToken"`
	Expiration      *string `json:"Expiration"`
	AccessKeySecret *string `json:"AccessKeySecret"`
	AccessKeyId     *string `json:"AccessKeyId"`
}

type assumeRoleResponse struct {
	RequestID       *string          `json:"RequestId"`
	AssumedRoleUser *assumedRoleUser `json:"AssumedRoleUser"`
	Credentials     *ossCredentials  `json:"Credentials"`
}

func (provider *ossOIDCCredentialsProvider) getCredentials() (session *sessionCredentials, err error) {
	req := &httpRequest{
		Method:   "POST",
		Protocol: "https",
		Host:     provider.stsEndpoint,
		Headers:  map[string]string{},
	}

	connectTimeout := 5 * time.Second
	readTimeout := 10 * time.Second

	if provider.httpOptions != nil && provider.httpOptions.connectTimeout > 0 {
		connectTimeout = time.Duration(provider.httpOptions.connectTimeout) * time.Millisecond
	}
	if provider.httpOptions != nil && provider.httpOptions.readTimeout > 0 {
		readTimeout = time.Duration(provider.httpOptions.readTimeout) * time.Millisecond
	}
	if provider.httpOptions != nil && provider.httpOptions.proxy != "" {
		req.Proxy = provider.httpOptions.proxy
	}
	req.ConnectTimeout = connectTimeout
	req.ReadTimeout = readTimeout

	queries := make(map[string]string)
	queries["Version"] = queryVersion
	queries["Action"] = queryAction
	queries["Format"] = qeuryFormat
	queries["Timestamp"] = getTimeInFormatISO8601()
	req.Queries = queries

	bodyForm := make(map[string]string)
	bodyForm["RoleArn"] = provider.roleARN
	bodyForm["OIDCProviderArn"] = provider.oidcProviderARN
	token, err := os.ReadFile(provider.oidcTokenFilePath)
	if err != nil {
		return
	}

	bodyForm["OIDCToken"] = string(token)
	if provider.policy != "" {
		bodyForm["Policy"] = provider.policy
	}
	bodyForm["RoleSessionName"] = provider.roleSessionName
	bodyForm["DurationSeconds"] = strconv.Itoa(provider.durationSeconds)
	req.Form = bodyForm

	// set headers
	req.Headers["Accept-Encoding"] = "identity"
	res, err := Do(req)
	if err != nil {
		return
	}

	if res.StatusCode != http.StatusOK {
		message := "get session token failed: "
		err = errors.New(message + string(res.Body))
		return
	}
	var data assumeRoleResponse
	err = json.Unmarshal(res.Body, &data)
	if err != nil {
		err = errors.Errorf("get oidc sts token err, json.Unmarshal fail: %s", err.Error())
		return
	}
	if data.Credentials == nil {
		err = errors.New("get oidc sts token err, fail to get credentials")
		return
	}

	if data.Credentials.AccessKeyId == nil || data.Credentials.AccessKeySecret == nil || data.Credentials.SecurityToken == nil {
		err = errors.New("refresh RoleArn sts token err, fail to get credentials")
		return
	}

	session = &sessionCredentials{
		AccessKeyId:     *data.Credentials.AccessKeyId,
		AccessKeySecret: *data.Credentials.AccessKeySecret,
		SecurityToken:   *data.Credentials.SecurityToken,
		Expiration:      *data.Credentials.Expiration,
	}
	return
}

func (provider *ossOIDCCredentialsProvider) needUpdateCredential() (result bool) {
	if provider.expirationTimestamp == 0 {
		return true
	}

	return provider.expirationTimestamp-time.Now().Unix() <= 180
}

func (provider *ossOIDCCredentialsProvider) Retrieve() (credentials.Value, error) {
	if provider.sessionCredentials == nil || provider.needUpdateCredential() {
		sessionCredentials, err := provider.getCredentials()
		if err != nil {
			return credentials.Value{}, err
		}

		provider.sessionCredentials = sessionCredentials
		expirationTime, err := time.Parse("2006-01-02T15:04:05Z", sessionCredentials.Expiration)
		if err != nil {
			return credentials.Value{}, err
		}

		provider.lastUpdateTimestamp = time.Now().Unix()
		provider.expirationTimestamp = expirationTime.Unix()
	}

	value := credentials.Value{
		AccessKeyID:     provider.sessionCredentials.AccessKeyId,
		SecretAccessKey: provider.sessionCredentials.AccessKeySecret,
		SessionToken:    provider.sessionCredentials.SecurityToken,
		ProviderName:    provider.GetProviderName(),
	}

	return value, nil
}

func (provider *ossOIDCCredentialsProvider) IsExpired() bool {
	return provider.needUpdateCredential()
}

func (provider *ossOIDCCredentialsProvider) GetProviderName() string {
	return oidcProviderName
}

func (provider *ossOIDCCredentialsProvider) GetCredentials(ctx context.Context) (alicreds.Credentials, error) {
	if provider.sessionCredentials == nil || provider.needUpdateCredential() {
		sessionCredentials, err := provider.getCredentials()
		if err != nil {
			return alicreds.Credentials{}, err
		}

		provider.sessionCredentials = sessionCredentials
		expirationTime, err := time.Parse("2006-01-02T15:04:05Z", sessionCredentials.Expiration)
		if err != nil {
			return alicreds.Credentials{}, err
		}

		provider.lastUpdateTimestamp = time.Now().Unix()
		provider.expirationTimestamp = expirationTime.Unix()
	}

	expires := time.Unix(provider.expirationTimestamp, 0)
	cred := alicreds.Credentials{
		AccessKeyID:     provider.sessionCredentials.AccessKeyId,
		AccessKeySecret: provider.sessionCredentials.AccessKeySecret,
		SecurityToken:   provider.sessionCredentials.SecurityToken,
		Expires:         &expires,
	}
	return cred, nil
}

type httpRequest struct {
	Method         string // http request method
	URL            string // http url
	Protocol       string // http or https
	Host           string // http host
	ReadTimeout    time.Duration
	ConnectTimeout time.Duration
	Proxy          string            // http proxy
	Form           map[string]string // http form
	Body           []byte            // request body for JSON or stream
	Path           string
	Queries        map[string]string
	Headers        map[string]string
}

func (req *httpRequest) BuildRequestURL() string {
	httpUrl := fmt.Sprintf("%s://%s%s", req.Protocol, req.Host, req.Path)
	if req.URL != "" {
		httpUrl = req.URL
	}

	querystring := getURLFormedMap(req.Queries)
	if querystring != "" {
		httpUrl = httpUrl + "?" + querystring
	}

	return fmt.Sprintf("%s %s", req.Method, httpUrl)
}

type Response struct {
	StatusCode int
	Headers    map[string]string
	Body       []byte
}

var newRequest = http.NewRequest

func Do(req *httpRequest) (res *Response, err error) {
	querystring := getURLFormedMap(req.Queries)
	// do request
	httpUrl := fmt.Sprintf("%s://%s%s?%s", req.Protocol, req.Host, req.Path, querystring)
	if req.URL != "" {
		httpUrl = req.URL
	}

	var body io.Reader
	if req.Method == "GET" {
		body = strings.NewReader("")
	} else {
		body = strings.NewReader(getURLFormedMap(req.Form))
	}

	httpRequest, err := newRequest(req.Method, httpUrl, body)
	if err != nil {
		return
	}

	if req.Form != nil {
		httpRequest.Header["Content-Type"] = []string{contentType}
	}

	for key, value := range req.Headers {
		if value != "" {
			httpRequest.Header.Set(key, value)
		}
	}

	httpClient := &http.Client{}

	if req.ReadTimeout != 0 {
		httpClient.Timeout = req.ReadTimeout + req.ConnectTimeout
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if req.Proxy != "" {
		var proxy *url.URL
		proxy, err = url.Parse(req.Proxy)
		if err != nil {
			return
		}
		transport.Proxy = http.ProxyURL(proxy)
	}

	if req.ConnectTimeout != 0 {
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{
				Timeout:   req.ConnectTimeout,
				DualStack: true,
			}).DialContext(ctx, network, address)
		}
	}

	httpClient.Transport = transport

	httpResponse, err := httpClient.Do(httpRequest)
	if err != nil {
		return
	}

	defer httpResponse.Body.Close()

	responseBody, err := io.ReadAll(httpResponse.Body)
	if err != nil {
		return
	}
	res = &Response{
		StatusCode: httpResponse.StatusCode,
		Headers:    make(map[string]string),
		Body:       responseBody,
	}
	for key, v := range httpResponse.Header {
		res.Headers[key] = v[0]
	}

	return
}

func getURLFormedMap(source map[string]string) (urlEncoded string) {
	urlEncoder := url.Values{}
	for key, value := range source {
		urlEncoder.Add(key, value)
	}
	urlEncoded = urlEncoder.Encode()
	return
}

func getTimeInFormatISO8601() (timeStr string) {
	gmt := time.FixedZone("GMT", 0)

	return time.Now().In(gmt).Format("2006-01-02T15:04:05Z")
}
