package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTeardownsEmpty(t *testing.T) {
	var tds Teardowns
	tds.Teardown()
}

func TestTeardownsOrder(t *testing.T) {
	var tds Teardowns
	var nums []int

	tds.Add(func() { nums = append(nums, 1) })
	tds.Add(func() { nums = append(nums, 2) })
	tds.Add(func() { nums = append(nums, 3) })

	tds.Teardown()
	require.EqualValues(t, []int{3, 2, 1}, nums)
}
