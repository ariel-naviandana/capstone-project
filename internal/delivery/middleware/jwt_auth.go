package middleware

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/logging"
	"github.com/capstone-b4/capstone-go/internal/pkg/response"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

const (
	ContextUserIDKey = "auth_user_id"
	ContextClaimsKey = "auth_claims"

	authHeader       = "Authorization"
	bearerPrefix     = "Bearer "
	signingAlgorithm = "HS256"
)

// Claims adalah payload JWT dengan user_id sebagai custom claim.
// Di-serialize sesuai JSON tag supaya konsisten antara generator dan parser.
type Claims struct {
	UserID int64 `json:"user_id"`
	jwt.RegisteredClaims
}

// GenerateToken menerbitkan JWT HS256 untuk userID dengan expiry dari config
// (default JWT_EXPIRY_HOUR=24 jam). Digunakan oleh endpoint login atau test.
// Error jika JWT_SECRET belum di-set atau userID tidak valid.
func GenerateToken(userID int64) (string, error) {
	if userID <= 0 {
		return "", errors.New("userID must be positive")
	}
	secret := config.AppConfig.JWTSecret
	if secret == "" {
		return "", errors.New("JWT_SECRET not configured")
	}
	expiry := config.AppConfig.JWTExpiryHour
	if expiry <= 0 {
		expiry = 24
	}

	claims := Claims{
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Duration(expiry) * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "capstone-b4",
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

// extractBearerToken validasi header Authorization dan return token string.
// Return error dengan detail siap dipakai di response 401.
func extractBearerToken(c *gin.Context) (string, string, string) {
	raw := c.GetHeader(authHeader)
	if raw == "" {
		return "", "missing Authorization header", ""
	}
	if !strings.HasPrefix(raw, bearerPrefix) {
		return "", "invalid Authorization format", "expected 'Bearer <token>'"
	}
	tok := strings.TrimSpace(strings.TrimPrefix(raw, bearerPrefix))
	if tok == "" {
		return "", "empty bearer token", ""
	}
	return tok, "", ""
}

// parseAndValidateToken verify signature, expiry, dan claim user_id.
// Return (claims, "", "") jika sukses, atau ("", msg, detail) jika gagal.
func parseAndValidateToken(tokenStr, secret string) (*Claims, string, string) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
		if t.Method.Alg() != signingAlgorithm {
			return nil, errors.New("unexpected signing method: " + t.Method.Alg())
		}
		return []byte(secret), nil
	})
	if err != nil {
		return nil, "invalid or expired token", err.Error()
	}
	if !token.Valid {
		return nil, "token signature invalid", ""
	}
	if claims.UserID <= 0 {
		return nil, "user_id claim missing or invalid", ""
	}
	return claims, "", ""
}

// JWTAuth memverifikasi Bearer token di header Authorization.
// Pada token valid: set user_id + claims ke gin.Context (keys:
// ContextUserIDKey, ContextClaimsKey) agar handler downstream bisa
// mengakses via c.GetInt64(ContextUserIDKey) atau helper GetAuthUserID.
// Response 401 ERR_UNAUTHORIZED jika:
//   - Header Authorization kosong
//   - Format bukan 'Bearer <token>'
//   - Signature invalid atau algoritma tidak match HS256
//   - Token expired
//   - Claim user_id kosong atau <= 0
func JWTAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		logger := logging.GetLogger(c)

		secret := config.AppConfig.JWTSecret
		if secret == "" {
			logger.Error().Msg("JWTAuth: JWT_SECRET is empty")
			c.AbortWithStatusJSON(http.StatusInternalServerError,
				response.ErrorJSON(response.ErrInternalError, "JWT secret not configured", ""))
			return
		}

		tokenStr, hdrMsg, hdrDetail := extractBearerToken(c)
		if hdrMsg != "" {
			logger.Warn().Str("reason", hdrMsg).Msg("JWTAuth: header invalid")
			c.AbortWithStatusJSON(http.StatusUnauthorized,
				response.ErrorJSON(response.ErrUnauthorized, hdrMsg, hdrDetail))
			return
		}

		claims, tokMsg, tokDetail := parseAndValidateToken(tokenStr, secret)
		if tokMsg != "" {
			logger.Warn().Str("reason", tokMsg).Msg("JWTAuth: token invalid")
			c.AbortWithStatusJSON(http.StatusUnauthorized,
				response.ErrorJSON(response.ErrUnauthorized, tokMsg, tokDetail))
			return
		}

		c.Set(ContextUserIDKey, claims.UserID)
		c.Set(ContextClaimsKey, claims)
		logger.Debug().
			Int64("auth_user_id", claims.UserID).
			Str("issuer", claims.Issuer).
			Msg("JWTAuth OK")
		c.Next()
	}
}

// GetAuthUserID ambil user_id dari context (di-set oleh JWTAuth).
// Return (0, false) jika tidak ada — artinya handler belum di belakang JWTAuth.
func GetAuthUserID(c *gin.Context) (int64, bool) {
	v, exists := c.Get(ContextUserIDKey)
	if !exists {
		return 0, false
	}
	id, ok := v.(int64)
	return id, ok
}
