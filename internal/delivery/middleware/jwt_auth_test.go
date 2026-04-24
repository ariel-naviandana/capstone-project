package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
)

const (
	testSecret               = "test-secret-do-not-use-in-prod"
	testProtectedPath        = "/protected"
	testBearerPrefix         = "Bearer "
	testMsgInvalidOrExpired  = "invalid or expired token"
)

func setupTestConfig(t *testing.T) {
	t.Helper()
	config.AppConfig.JWTSecret = testSecret
	config.AppConfig.JWTExpiryHour = 1
}

// buildTestRouter bikin gin router dengan endpoint /protected di belakang JWTAuth.
// Handler downstream return user_id dari context sebagai verifikasi bahwa
// middleware benar-benar set-value (bukan cuma passthrough).
// Logger akan fallback ke global zerolog (lihat logging.GetLogger),
// jadi tidak perlu inject apapun.
func buildTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET(testProtectedPath, JWTAuth(), func(c *gin.Context) {
		uid, _ := GetAuthUserID(c)
		c.JSON(http.StatusOK, gin.H{"user_id": uid})
	})
	return r
}

func TestGenerateToken(t *testing.T) {
	setupTestConfig(t)

	t.Run("Success", func(t *testing.T) {
		token, err := GenerateToken(42)
		assert.NoError(t, err)
		assert.NotEmpty(t, token)

		parsed, err := jwt.ParseWithClaims(token, &Claims{}, func(*jwt.Token) (interface{}, error) {
			return []byte(testSecret), nil
		})
		assert.NoError(t, err)
		assert.True(t, parsed.Valid)
		claims := parsed.Claims.(*Claims)
		assert.Equal(t, int64(42), claims.UserID)
		assert.Equal(t, "capstone-b4", claims.Issuer)
	})

	t.Run("RejectsZeroUserID", func(t *testing.T) {
		_, err := GenerateToken(0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "userID must be positive")
	})

	t.Run("RejectsNegativeUserID", func(t *testing.T) {
		_, err := GenerateToken(-5)
		assert.Error(t, err)
	})

	t.Run("RejectsEmptySecret", func(t *testing.T) {
		config.AppConfig.JWTSecret = ""
		defer func() { config.AppConfig.JWTSecret = testSecret }()
		_, err := GenerateToken(1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "JWT_SECRET")
	})
}

func TestJWTAuth_ValidToken(t *testing.T) {
	setupTestConfig(t)
	r := buildTestRouter()

	token, err := GenerateToken(123)
	assert.NoError(t, err)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", testProtectedPath, nil)
	req.Header.Set("Authorization", testBearerPrefix+token)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), `"user_id":123`)
}

func TestJWTAuth_MissingHeader(t *testing.T) {
	setupTestConfig(t)
	r := buildTestRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", testProtectedPath, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "ERR_UNAUTHORIZED")
	assert.Contains(t, w.Body.String(), "missing Authorization header")
}

func TestJWTAuth_InvalidFormat(t *testing.T) {
	setupTestConfig(t)
	r := buildTestRouter()

	cases := []struct {
		name   string
		header string
		msg    string
	}{
		{"NoBearerPrefix", "xxx.yyy.zzz", "invalid Authorization format"},
		{"BasicAuth", "Basic dXNlcjpwYXNz", "invalid Authorization format"},
		{"EmptyBearer", testBearerPrefix, "empty bearer token"},
		{"BearerWithSpaces", testBearerPrefix + "   ", "empty bearer token"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			req, _ := http.NewRequest("GET", testProtectedPath, nil)
			req.Header.Set("Authorization", tc.header)
			r.ServeHTTP(w, req)
			assert.Equal(t, http.StatusUnauthorized, w.Code)
			assert.Contains(t, w.Body.String(), tc.msg)
		})
	}
}

func TestJWTAuth_InvalidSignature(t *testing.T) {
	setupTestConfig(t)
	r := buildTestRouter()

	// Generate token with different secret
	claims := Claims{
		UserID: 1,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	badToken, _ := tok.SignedString([]byte("different-secret"))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", testProtectedPath, nil)
	req.Header.Set("Authorization", testBearerPrefix+badToken)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), testMsgInvalidOrExpired)
}

func TestJWTAuth_ExpiredToken(t *testing.T) {
	setupTestConfig(t)
	r := buildTestRouter()

	claims := Claims{
		UserID: 1,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-1 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now().Add(-2 * time.Hour)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	expired, _ := tok.SignedString([]byte(testSecret))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", testProtectedPath, nil)
	req.Header.Set("Authorization", testBearerPrefix+expired)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), testMsgInvalidOrExpired)
}

func TestJWTAuth_WrongAlgorithm(t *testing.T) {
	setupTestConfig(t)
	r := buildTestRouter()

	// Token dengan algoritma 'none' — serangan umum pada JWT parser yang loose
	claims := Claims{
		UserID: 1,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
	noneToken, _ := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", testProtectedPath, nil)
	req.Header.Set("Authorization", testBearerPrefix+noneToken)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "ERR_UNAUTHORIZED")
}

func TestJWTAuth_UserIDZero(t *testing.T) {
	setupTestConfig(t)
	r := buildTestRouter()

	// Token dengan user_id=0 (missing claim di-unmarshal sebagai 0)
	claims := Claims{
		UserID: 0,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	zeroToken, _ := tok.SignedString([]byte(testSecret))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", testProtectedPath, nil)
	req.Header.Set("Authorization", testBearerPrefix+zeroToken)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), "user_id claim missing or invalid")
}

func TestJWTAuth_ServerSecretNotConfigured(t *testing.T) {
	config.AppConfig.JWTSecret = ""
	defer func() { config.AppConfig.JWTSecret = testSecret }()

	r := buildTestRouter()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", testProtectedPath, nil)
	req.Header.Set("Authorization", testBearerPrefix+"any.token.value")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "JWT secret not configured")
}

func TestJWTAuth_GarbageToken(t *testing.T) {
	setupTestConfig(t)
	r := buildTestRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", testProtectedPath, nil)
	req.Header.Set("Authorization", testBearerPrefix+"not-a-real-jwt")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Body.String(), testMsgInvalidOrExpired)
}

func TestGetAuthUserID_NotPresent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	id, ok := GetAuthUserID(c)
	assert.Equal(t, int64(0), id)
	assert.False(t, ok)
}

func TestGetAuthUserID_Present(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set(ContextUserIDKey, int64(77))
	id, ok := GetAuthUserID(c)
	assert.Equal(t, int64(77), id)
	assert.True(t, ok)
}
