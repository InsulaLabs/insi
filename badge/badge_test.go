package badge

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"testing"
)

func TestBadge(t *testing.T) {
	badge, err := BuildBadge(
		WithID("test"),
		WithCurveSelector(BadgeCurveSelectorP256))
	if err != nil {
		t.Fatal(err)
	}

	some_data := "some data"
	signed, err := badge.Sign(&some_data)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("Original data: %s\n", some_data)
	fmt.Printf("Signed data: %+v\n", signed)

	verified, err := badge.Verify(signed)
	if err != nil {
		t.Fatal(err)
	}

	if !verified {
		t.Fatal("verified is false")
	}
}

func TestBadgeEncrypt(t *testing.T) {
	badge, err := BuildBadge(
		WithID("test"),
		WithCurveSelector(BadgeCurveSelectorP256))
	if err != nil {
		t.Fatal(err)
	}

	secret := []byte("0123456789ABCDEF") // 16-byte key for AES-128
	encrypted, err := badge.EncryptBadge(secret)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("Encrypted badge: %+v\n", encrypted)

	decrypted, err := FromEncryptedBadge(secret, encrypted)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("Decrypted badge: %+v\n", decrypted)
}

func randomString(length int) string {
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func TestBadgeEncryptAndDecrypt(t *testing.T) {

	type testCase struct {
		CurveSelector BadgeCurveSelector
		Secret        []byte
		Data          string
	}

	testCases := []testCase{
		{BadgeCurveSelectorP256, []byte(randomString(16)), randomString(255)},
		{BadgeCurveSelectorP384, []byte(randomString(32)), randomString(255)},
		{BadgeCurveSelectorP521, []byte(randomString(64)), randomString(255)},
	}

	for _, testCase := range testCases {
		badge, err := BuildBadge(
			WithID("test"),
			WithCurveSelector(testCase.CurveSelector))
		if err != nil {
			t.Fatal(err)
		}

		badge2, err := BuildBadge(
			WithID("test2"),
			WithCurveSelector(testCase.CurveSelector))
		if err != nil {
			t.Fatal(err)
		}

		fmt.Printf("Badge: %+v\n", badge)

		signed, err := badge.Sign(&testCase.Data)
		if err != nil {
			t.Fatal(err)
		}

		verificationOne, err := badge.Verify(signed)
		if err != nil {
			t.Fatal(err)
		}

		if !verificationOne {
			t.Fatal("verificationOne is false")
		}

		failedVerification, err := badge2.Verify(signed)
		if err == nil {
			t.Fatal("expected error from different badge verification")
		}
		if failedVerification {
			t.Fatal("failedVerification is true")
		}

		encrypted, err := badge.EncryptBadge(testCase.Secret)
		if err != nil {
			t.Fatal(err)
		}

		badge64 := base64.StdEncoding.EncodeToString(encrypted)
		fmt.Printf("Encrypted badge: %s\n", badge64)

		decrypted, err := FromEncryptedBadge(testCase.Secret, encrypted)
		if err != nil {
			t.Fatal(err)
		}

		if decrypted.GetID() != badge.GetID() {
			t.Fatal("decrypted badge ID does not match original badge ID")
		}

		if decrypted.GetVersion() != badge.GetVersion() {
			t.Fatal("decrypted badge version does not match original badge version")
		}

		verificationTwo, err := decrypted.Verify(signed)
		if err != nil {
			t.Fatal(err)
		}

		if !verificationTwo {
			t.Fatal("verificationTwo is false [verification of signed data post flattening and expansion]")
		}

		otherEncrypted, err := badge2.EncryptBadge(testCase.Secret)
		if err != nil {
			t.Fatal(err)
		}

		otherDecrypted, err := FromEncryptedBadge(testCase.Secret, otherEncrypted)
		if err != nil {
			t.Fatal(err)
		}

		otherVerification, err := otherDecrypted.Verify(signed)
		if err == nil {
			t.Fatal("expected error from other badge verification")
		}

		if otherVerification {
			t.Fatal("otherVerification is true [verification of signed data post flattening and expansion]")
		}
	}
}
