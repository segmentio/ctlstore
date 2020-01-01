package schema

import (
	"fmt"
	"strings"
	"testing"
)

func TestNormalizeFamilyName(t *testing.T) {
	suite := []struct {
		desc      string
		input     string
		expectStr string
		expectErr error
	}{
		{"Lowers", "LOWER", "lower", nil},
		{"Too short", "ab", "", ErrFamilyNameTooShort},
		{"Too long", strings.Repeat("a", 31), "", ErrFamilyNameTooLong},
		{"Invalid chars", "abc-123", "", ErrFamilyNameInvalid},
		{"Starts with number", "1abc", "", ErrFamilyNameInvalid},
		{"Contains multi-underscore", "a__b", "", ErrFamilyNameInvalid},
	}

	for i, testCase := range suite {
		testName := fmt.Sprintf("%d %s", i, testCase.desc)
		t.Run(testName, func(t *testing.T) {
			gotStr, gotErr := normalizeFamilyName(testCase.input)
			if want, got := testCase.expectErr, gotErr; want != got {
				t.Errorf("Expected error %v, got %v", want, got)
			}
			if want, got := testCase.expectStr, gotStr; want != got {
				t.Errorf("Expected %v, got %v", want, got)
			}
		})
	}
}

func TestNormalizeTableName(t *testing.T) {
	suite := []struct {
		desc      string
		input     string
		expectStr string
		expectErr error
	}{
		{"Lowers", "LOWER", "lower", nil},
		{"Too short", "ab", "", ErrTableNameTooShort},
		{"Too long", strings.Repeat("a", 51), "", ErrTableNameTooLong},
		{"Invalid chars", "abc-123", "", ErrTableNameInvalid},
		{"Starts with number", "1abc", "", ErrTableNameInvalid},
		{"Contains multi-underscore", "a__b", "", ErrTableNameInvalid},
	}

	for i, testCase := range suite {
		testName := fmt.Sprintf("%d %s", i, testCase.desc)
		t.Run(testName, func(t *testing.T) {
			gotStr, gotErr := normalizeTableName(testCase.input)
			if want, got := testCase.expectErr, gotErr; want != got {
				t.Errorf("Expected error %v, got %v", want, got)
			}
			if want, got := testCase.expectStr, gotStr; want != got {
				t.Errorf("Expected %v, got %v", want, got)
			}
		})
	}
}

func TestNormalizeFieldName(t *testing.T) {
	suite := []struct {
		desc      string
		input     string
		expectStr string
		expectErr error
	}{
		{"Lowers", "LOWER", "lower", nil},
		{"Too short", "", "", ErrFieldNameTooShort},
		{"Too long", strings.Repeat("a", 100), "", ErrFieldNameTooLong},
		{"Invalid chars", "abc-123", "", ErrFieldNameInvalid},
		{"Starts with number", "1abc", "", ErrFieldNameInvalid},
	}

	for i, testCase := range suite {
		testName := fmt.Sprintf("%d %s", i, testCase.desc)
		t.Run(testName, func(t *testing.T) {
			gotStr, gotErr := normalizeFieldName(testCase.input)
			if want, got := testCase.expectErr, gotErr; want != got {
				t.Errorf("Expected error %v, got %v", want, got)
			}
			if want, got := testCase.expectStr, gotStr; want != got {
				t.Errorf("Expected %v, got %v", want, got)
			}
		})
	}
}

func TestValidateWriterName(t *testing.T) {
	suite := []struct {
		desc      string
		input     string
		expectStr string
		expectErr error
	}{
		{"Too short", "x", "", ErrWriterNameTooShort},
		{"Too long", strings.Repeat("a", 51), "", ErrWriterNameTooLong},
	}

	for i, testCase := range suite {
		testName := fmt.Sprintf("%d %s", i, testCase.desc)
		t.Run(testName, func(t *testing.T) {
			gotStr, gotErr := validateWriterName(testCase.input)
			if want, got := testCase.expectErr, gotErr; want != got {
				t.Errorf("Expected error %v, got %v", want, got)
			}
			if want, got := testCase.expectStr, gotStr; want != got {
				t.Errorf("Expected %v, got %v", want, got)
			}
		})
	}
}
