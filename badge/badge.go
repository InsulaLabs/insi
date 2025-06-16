/*
	Author: bosley

	This is a simple badge system that allows for signing and verifying data/
	maintaining node identity.
*/

package badge

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"time"

	"github.com/google/uuid"
)

var BadgePrefix = "tek"

type BadgeErrorCode int

const (
	BadgeErrorSecretRequired BadgeErrorCode = iota
	BadgeErrorDataRequired
	BadgeErrorPublicKeyRequired
	BadgeErrorSignatureRequired
	BadgeErrorInvalidSignature
	BadgeErrorInvalidPublicKey
	BadgeErrorMessageRequired
	BadgeErrorInvalidHash
	BadgeErrorInvalidCurve
	BadgeErrorInvalidPrivateKey
)

type BadgeCurveSelector int

const (
	BadgeCurveSelectorUNSET BadgeCurveSelector = iota
	BadgeCurveSelectorP256
	BadgeCurveSelectorP384
	BadgeCurveSelectorP521
)

type BadgeVersion int

const (
	BadgeVersionUNSET BadgeVersion = iota
	BadgeVersion1
)

func (e BadgeErrorCode) String() string {
	switch e {
	case BadgeErrorSecretRequired:
		return "secret is required"
	case BadgeErrorDataRequired:
		return "data is required"
	case BadgeErrorPublicKeyRequired:
		return "public key is required"
	case BadgeErrorSignatureRequired:
		return "signature is required"
	case BadgeErrorInvalidSignature:
		return "invalid signature"
	case BadgeErrorInvalidPublicKey:
		return "invalid public key"
	case BadgeErrorMessageRequired:
		return "message is required"
	case BadgeErrorInvalidHash:
		return "invalid hash"
	case BadgeErrorInvalidCurve:
		return "invalid curve"
	case BadgeErrorInvalidPrivateKey:
		return "invalid private key"
	default:
		return "unknown error"
	}
}

type BadgeError struct {
	Code    BadgeErrorCode
	Message string
}

func (e *BadgeError) Error() string {
	return e.Message
}

func NewBadgeErrorWithMessage(code BadgeErrorCode, message string) *BadgeError {
	return &BadgeError{Code: code, Message: message}
}

func NewBadgeError(code BadgeErrorCode) *BadgeError {
	return NewBadgeErrorWithMessage(code, code.String())
}

type SignedPackage struct {
	PublicKey []byte `json:"public_key"`
	Signature []byte `json:"signature"`
	Message   []byte `json:"message"`
}

type EncryptedBadgeData struct {
	ID            string             `json:"id"`
	Version       BadgeVersion       `json:"version"`
	B64PublicKey  string             `json:"public_key"`
	B64PrivateKey string             `json:"private_key"`
	Curve         BadgeCurveSelector `json:"curve"`
	DateEncrypted time.Time          `json:"date_encrypted"`
}

type EncryptedBadge struct {
	Hash []byte `json:"hash"`
	Data []byte `json:"data"`
}

type Badge interface {
	GetVersion() BadgeVersion
	GetID() string

	Sign(data *string) (SignedPackage, error)
	Verify(signedPackage SignedPackage) (bool, error)

	EncryptBadge(secret []byte) ([]byte, error)

	// Using badge private key
	EncryptData(data []byte) ([]byte, error)
	DecryptData(data []byte) ([]byte, error)
}

type BadgeOption func(*builder) BadgeOption

func WithID(id string) BadgeOption {
	return func(b *builder) BadgeOption {
		prev := b.ID
		b.ID = id
		return WithID(prev)
	}
}

func WithCurveSelector(curveSelector BadgeCurveSelector) BadgeOption {
	return func(b *builder) BadgeOption {
		b.CurveSelector = curveSelector
		return WithCurveSelector(curveSelector)
	}
}

func BuildBadge(options ...BadgeOption) (Badge, error) {
	badge := &builder{}

	if badge.ID == "" {
		badge.ID = uuid.New().String()
	}

	if badge.CurveSelector == BadgeCurveSelectorUNSET {
		badge.CurveSelector = BadgeCurveSelectorP256
	}

	switch badge.CurveSelector {
	case BadgeCurveSelectorP256:
		badge.Curve = elliptic.P256()
	case BadgeCurveSelectorP384:
		badge.Curve = elliptic.P384()
	case BadgeCurveSelectorP521:
		badge.Curve = elliptic.P521()
	}

	privateKey, err := generateKeyPair(badge.Curve)
	if err != nil {
		return nil, err
	}
	badge.Key = privateKey

	return badge, nil
}

func generateKeyPair(curve elliptic.Curve) (*ecdsa.PrivateKey, error) {
	privateKey, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		return nil, err
	}
	return privateKey, nil
}

func encrypt(key, data []byte) ([]byte, error) {
	hash := sha256.Sum256(key)
	aesKey := hash[:]

	blockCipher, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(blockCipher)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	return ciphertext, nil
}

func decrypt(key, data []byte) ([]byte, error) {
	// Derive a 32-byte key using SHA-256
	hash := sha256.Sum256(key)
	aesKey := hash[:]

	blockCipher, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(blockCipher)
	if err != nil {
		return nil, err
	}
	nonce, ciphertext := data[:gcm.NonceSize()], data[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}

type builder struct {
	ID            string
	CurveSelector BadgeCurveSelector
	Curve         elliptic.Curve
	Key           *ecdsa.PrivateKey
	Version       BadgeVersion
}

var _ Badge = &builder{}

func (x *builder) GetID() string {
	return x.ID
}

func (x *builder) GetVersion() BadgeVersion {
	return x.Version
}

func (x *builder) MarshalPublicKey() []byte {
	return elliptic.MarshalCompressed(x.Curve, x.Key.PublicKey.X, x.Key.PublicKey.Y)
}

func (x *builder) UnmarshalPublicKey(data []byte) (*ecdsa.PublicKey, error) {
	px, py := elliptic.UnmarshalCompressed(x.Curve, data)
	if px == nil || py == nil {
		return nil, NewBadgeError(BadgeErrorInvalidPublicKey)
	}
	return &ecdsa.PublicKey{Curve: x.Curve, X: px, Y: py}, nil
}

func (b *builder) Sign(data *string) (SignedPackage, error) {
	if data == nil {
		return SignedPackage{}, NewBadgeError(BadgeErrorDataRequired)
	}
	if len(*data) == 0 {
		return SignedPackage{}, NewBadgeError(BadgeErrorDataRequired)
	}
	hash := sha256.Sum256([]byte(*data))
	signature, err := ecdsa.SignASN1(rand.Reader, b.Key, hash[:])
	if err != nil {
		return SignedPackage{}, err
	}
	return SignedPackage{PublicKey: b.MarshalPublicKey(), Signature: signature, Message: []byte(*data)}, nil
}

func (b *builder) Verify(signedPackage SignedPackage) (bool, error) {
	if signedPackage.PublicKey == nil {
		return false, NewBadgeError(BadgeErrorPublicKeyRequired)
	}
	if signedPackage.Signature == nil {
		return false, NewBadgeError(BadgeErrorSignatureRequired)
	}
	if signedPackage.Message == nil {
		return false, NewBadgeError(BadgeErrorMessageRequired)
	}

	badgePublicKey := b.MarshalPublicKey()
	if !bytes.Equal(badgePublicKey, signedPackage.PublicKey) {
		return false, NewBadgeError(BadgeErrorInvalidPublicKey)
	}

	publicKey, err := b.UnmarshalPublicKey(signedPackage.PublicKey)
	if err != nil {
		return false, NewBadgeError(BadgeErrorInvalidPublicKey)
	}

	hash := sha256.Sum256(signedPackage.Message)
	return ecdsa.VerifyASN1(publicKey, hash[:], signedPackage.Signature), nil
}

func (b *builder) EncryptBadge(secret []byte) ([]byte, error) {
	publicKey := b.MarshalPublicKey()
	publicKeyBase64 := base64.StdEncoding.EncodeToString(publicKey)

	x509Encoded, err := x509.MarshalPKCS8PrivateKey(b.Key)
	if err != nil {
		return nil, err
	}
	privateKey := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: x509Encoded}))
	privateKeyBase64 := base64.StdEncoding.EncodeToString([]byte(privateKey))

	encryptedBadgeData := &EncryptedBadgeData{
		ID:            b.ID,
		Version:       b.Version,
		Curve:         b.CurveSelector,
		DateEncrypted: time.Now(),
		B64PublicKey:  publicKeyBase64,
		B64PrivateKey: privateKeyBase64,
	}

	encryptedBadgeBytes, err := json.Marshal(encryptedBadgeData)
	if err != nil {
		return nil, err
	}

	hash := sha256.Sum256(encryptedBadgeBytes)

	encryptedBadge := &EncryptedBadge{
		Hash: hash[:],
		Data: encryptedBadgeBytes,
	}

	encBb, err := json.Marshal(encryptedBadge)
	if err != nil {
		return nil, err
	}

	encrypted, err := encrypt(secret, encBb)
	if err != nil {
		return nil, err
	}

	return encrypted, nil
}

func FromEncryptedBadge(secret []byte, encryptedBadge []byte) (Badge, error) {

	decryptedBadgeBytes, err := decrypt(secret, encryptedBadge)
	if err != nil {
		return nil, err
	}

	encryptedBadgeData := &EncryptedBadge{}
	err = json.Unmarshal(decryptedBadgeBytes, encryptedBadgeData)
	if err != nil {
		return nil, err
	}

	hash := sha256.Sum256(encryptedBadgeData.Data)
	if !bytes.Equal(hash[:], encryptedBadgeData.Hash) {
		return nil, NewBadgeError(BadgeErrorInvalidHash)
	}

	badgeData := &EncryptedBadgeData{}
	err = json.Unmarshal(encryptedBadgeData.Data, badgeData)
	if err != nil {
		return nil, err
	}

	var curve elliptic.Curve
	switch badgeData.Curve {
	case BadgeCurveSelectorP256:
		curve = elliptic.P256()
	case BadgeCurveSelectorP384:
		curve = elliptic.P384()
	case BadgeCurveSelectorP521:
		curve = elliptic.P521()
	default:
		return nil, NewBadgeError(BadgeErrorInvalidCurve)
	}

	unencodedPrivateKey, err := base64.StdEncoding.DecodeString(badgeData.B64PrivateKey)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(unencodedPrivateKey)
	if block == nil {
		return nil, NewBadgeError(BadgeErrorInvalidPrivateKey)
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	var privateKey *ecdsa.PrivateKey

	if key, ok := key.(*ecdsa.PrivateKey); ok {
		privateKey = key
	} else {
		return nil, NewBadgeError(BadgeErrorInvalidPrivateKey)
	}

	badge := &builder{
		ID:            badgeData.ID,
		CurveSelector: badgeData.Curve,
		Curve:         curve,
		Key:           privateKey,
		Version:       badgeData.Version,
	}

	return badge, nil
}

func (b *builder) EncryptData(data []byte) ([]byte, error) {
	return encrypt(b.Key.D.Bytes(), data)
}

func (b *builder) DecryptData(data []byte) ([]byte, error) {
	return decrypt(b.Key.D.Bytes(), data)
}
