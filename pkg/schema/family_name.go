package schema

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type FamilyName struct {
	Name string
}

const (
	MinFamilyNameLength = 3
	MaxFamilyNameLength = 30
)

var (
	ErrFamilyNameInvalid  = errors.New("Family names must be only letters, numbers, and single underscore")
	ErrFamilyNameTooLong  = fmt.Errorf("Family names can only be up to %d characters", MaxFamilyNameLength)
	ErrFamilyNameTooShort = fmt.Errorf("Family names must be at least %d characters", MinFamilyNameLength)
)

var familyNameChars = regexp.MustCompile("^$|^[a-z][a-z0-9_]*$")

func NewFamilyName(name string) (FamilyName, error) {
	normalized, err := normalizeFamilyName(name)
	if err != nil {
		return FamilyName{}, err
	}
	return FamilyName{normalized}, nil
}

func (fn FamilyName) String() string {
	return fn.Name
}

func normalizeFamilyName(familyName string) (string, error) {
	lowered := strings.ToLower(familyName)
	if strings.Contains(lowered, "__") {
		return "", ErrFamilyNameInvalid
	}
	if !familyNameChars.MatchString(lowered) {
		return "", ErrFamilyNameInvalid
	}
	if len(lowered) > MaxFamilyNameLength {
		return "", ErrFamilyNameTooLong
	}
	if len(lowered) < MinFamilyNameLength {
		return "", ErrFamilyNameTooShort
	}
	return lowered, nil
}
