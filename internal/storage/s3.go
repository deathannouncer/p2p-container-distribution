package storage

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// S3 is a minimal AWS S3 client implemented with only the standard library
// (net/http + crypto/hmac + crypto/sha256), signing requests with AWS
// Signature Version 4. It's used as the cold/overflow tier: chunks that
// don't fit the configured local cache budget spill here instead of being
// evicted outright.
//
// This intentionally avoids the AWS SDK so the node binary has zero
// third-party dependencies -- everything it needs to talk to S3 is ~200
// lines of stdlib HTTP + signing.
type S3 struct {
	Bucket          string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string // optional, for temporary STS credentials
	Endpoint        string // optional override, e.g. for S3-compatible stores (MinIO)
	httpClient      *http.Client
}

// NewS3 builds an S3-backed storage client from explicit credentials.
// Typically these come from environment variables (AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN) -- see cmd/node/main.go.
func NewS3(bucket, region, accessKeyID, secretAccessKey, sessionToken string) *S3 {
	return &S3{
		Bucket:          bucket,
		Region:          region,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		SessionToken:    sessionToken,
		httpClient:      &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *S3) endpoint() string {
	if s.Endpoint != "" {
		return s.Endpoint
	}
	return fmt.Sprintf("https://%s.s3.%s.amazonaws.com", s.Bucket, s.Region)
}

func (s *S3) objectKey(chunkID string) string {
	if len(chunkID) < 2 {
		return "chunks/_short/" + chunkID
	}
	return "chunks/" + chunkID[:2] + "/" + chunkID
}

func (s *S3) Put(chunkID string, data []byte) error {
	req, err := s.newRequest(http.MethodPut, s.objectKey(chunkID), data)
	if err != nil {
		return err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("storage: s3 put %s: %w", chunkID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("storage: s3 put %s: status %d: %s", chunkID, resp.StatusCode, body)
	}
	return nil
}

func (s *S3) Get(chunkID string) ([]byte, error) {
	req, err := s.newRequest(http.MethodGet, s.objectKey(chunkID), nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("storage: s3 get %s: %w", chunkID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("storage: s3 get %s: status %d: %s", chunkID, resp.StatusCode, body)
	}
	return io.ReadAll(resp.Body)
}

func (s *S3) Has(chunkID string) bool {
	req, err := s.newRequest(http.MethodHead, s.objectKey(chunkID), nil)
	if err != nil {
		return false
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode/100 == 2
}

func (s *S3) Delete(chunkID string) error {
	req, err := s.newRequest(http.MethodDelete, s.objectKey(chunkID), nil)
	if err != nil {
		return err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("storage: s3 delete %s: %w", chunkID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("storage: s3 delete %s: status %d: %s", chunkID, resp.StatusCode, body)
	}
	return nil
}

// List is not commonly needed for the S3 tier (it's overflow, not the
// source of truth for membership) but is implemented via ListObjectsV2 for
// completeness, e.g. cluster rebuild after total local-disk loss.
func (s *S3) List() ([]string, error) {
	req, err := s.newRequest(http.MethodGet, "", nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Set("list-type", "2")
	q.Set("prefix", "chunks/")
	req.URL.RawQuery = q.Encode()
	// Query string participates in the signature, so re-sign after setting it.
	if err := s.sign(req, nil); err != nil {
		return nil, err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("storage: s3 list: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("storage: s3 list: status %d: %s", resp.StatusCode, body)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return parseListKeys(body), nil
}

// parseListKeys does a minimal extraction of <Key>...</Key> entries from a
// ListObjectsV2 XML response without pulling in an XML dependency.
func parseListKeys(xmlBody []byte) []string {
	const openTag, closeTag = "<Key>", "</Key>"
	var keys []string
	s := string(xmlBody)
	for {
		i := strings.Index(s, openTag)
		if i < 0 {
			break
		}
		s = s[i+len(openTag):]
		j := strings.Index(s, closeTag)
		if j < 0 {
			break
		}
		full := s[:j]
		// strip "chunks/xx/" prefix back down to the bare chunk ID
		if parts := strings.Split(full, "/"); len(parts) == 3 {
			keys = append(keys, parts[2])
		} else {
			keys = append(keys, full)
		}
		s = s[j+len(closeTag):]
	}
	return keys
}

func (s *S3) newRequest(method, key string, body []byte) (*http.Request, error) {
	u := s.endpoint() + "/"
	if key != "" {
		u += url.PathEscape(key)
		// url.PathEscape escapes '/' too, which we don't want for the key path.
		u = strings.ReplaceAll(u, "%2F", "/")
	}
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, u, bodyReader)
	if err != nil {
		return nil, err
	}
	if err := s.sign(req, body); err != nil {
		return nil, err
	}
	return req, nil
}

// sign implements AWS Signature Version 4 for a single request using only
// crypto/sha256 and crypto/hmac from the standard library.
func (s *S3) sign(req *http.Request, body []byte) error {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	payloadHash := sha256Hex(body)
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if s.SessionToken != "" {
		req.Header.Set("x-amz-security-token", s.SessionToken)
	}
	req.Header.Set("Host", req.URL.Host)

	canonicalHeaders, signedHeaders := canonicalizeHeaders(req)
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL.Path),
		req.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, s.Region)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	signingKey := deriveSigningKey(s.SecretAccessKey, dateStamp, s.Region, "s3")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	authHeader := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		s.AccessKeyID, credentialScope, signedHeaders, signature,
	)
	req.Header.Set("Authorization", authHeader)
	return nil
}

func canonicalURI(p string) string {
	if p == "" {
		return "/"
	}
	return p
}

func canonicalizeHeaders(req *http.Request) (canonical string, signed string) {
	// Only Host and x-amz-* headers participate, which is sufficient (and
	// required) for S3 SigV4 requests of this shape.
	type kv struct{ k, v string }
	var headers []kv
	headers = append(headers, kv{"host", req.Header.Get("Host")})
	for name, vals := range req.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-") {
			headers = append(headers, kv{lower, strings.Join(vals, ",")})
		}
	}
	sort.Slice(headers, func(i, j int) bool { return headers[i].k < headers[j].k })

	var cb strings.Builder
	var names []string
	for _, h := range headers {
		fmt.Fprintf(&cb, "%s:%s\n", h.k, strings.TrimSpace(h.v))
		names = append(names, h.k)
	}
	return cb.String(), strings.Join(names, ";")
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func deriveSigningKey(secret, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

var _ Backend = (*S3)(nil)
