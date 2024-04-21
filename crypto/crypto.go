package encrypto

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
)

func Decrypt(data string) (string, error) {
	keyString := "8BZ3pCTp71LX5I//QsBYdz7w4JHXNVehSBXuXnScdqg="
	ivString := "ERttwv7oQb/KUQVBiJZvtA=="

	key, err := base64.StdEncoding.DecodeString(keyString)
	if err != nil {
		return "", err
	}
	iv, err := base64.StdEncoding.DecodeString(ivString)
	if err != nil {
		return "", err
	}

	ciphertext, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	mode := cipher.NewCTR(block, iv)
	decrypted := make([]byte, len(ciphertext))
	mode.XORKeyStream(decrypted, ciphertext)

	// If no conversion is needed, return the decrypted string
	return string(decrypted), nil
}
