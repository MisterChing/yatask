package yatask

import "github.com/google/uuid"

func GenUniqueID() string {
	return uuid.New().String()
}
