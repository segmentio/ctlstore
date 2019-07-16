package cmd

import (
	"path"

	"github.com/segmentio/ctlstore"
	"github.com/segmentio/ctlstore/pkg/executive"
	"github.com/segmentio/ctlstore/pkg/ldb"
	"github.com/spf13/cobra"
)

const (
	keyFamily                 = "family"
	keyFamilyShort            = "f"
	keyTable                  = "table"
	keyTableShort             = "t"
	keyLDB                    = "ldb"
	keyLDBShort               = "l"
	keyQuiet                  = "quiet"
	keyQuietShort             = "q"
	keyExecutiveLocation      = "executive"
	keyExecutiveLocationShort = "e"
	keyFields                 = "field"
	keyKeyFields              = "key-field"
	keyMaxSize                = "max-size"
	keyWarnSize               = "warn-size"
	keyWriter                 = "writer"
	keyRowsPerMinute          = "rows-per-minute"
	keyDataDogAddress         = "dogstatsd.address"
	keyDataDogBufferSize      = "dogstatsd.buffer-size"
	keyDataDogTags            = "dogstatsd.tags"
	keyCTLDBAddress           = "ctldb-address"
	keySoRAddress             = "sor-address"
)

var (
	defaultLDBPath = path.Join(ctlstore.DefaultCtlstorePath, ldb.DefaultLDBFilename)
)

func useFlagFields(cmd *cobra.Command) {
	cmd.Flags().StringArray(keyFields, nil, "the fields of the table of the form [name]:[type]")
}

func useFlagKeyFields(cmd *cobra.Command) {
	cmd.Flags().StringArray(keyKeyFields, nil, "the names of the fields that should serve as primary keys")
}

func useFlagExecutive(cmd *cobra.Command) {
	cmd.Flags().StringP(keyExecutiveLocation, keyExecutiveLocationShort, executive.DefaultExecutiveURL, "the location of the executive service")
}

func useFlagFamily(cmd *cobra.Command) {
	cmd.Flags().StringP(keyFamily, keyFamilyShort, "", "the name of the family")
}

func useFlagTable(cmd *cobra.Command) {
	cmd.Flags().StringP(keyTable, keyTableShort, "", "the name of the table")
}

func useFlagLDB(cmd *cobra.Command) {
	cmd.Flags().StringP(keyLDB, keyLDBShort, defaultLDBPath, "the path to the ldb")
}

func useFlagQuiet(cmd *cobra.Command) {
	cmd.Flags().BoolP(keyQuiet, keyQuietShort, false, "omit header output")
}

func useFlagMaxSize(cmd *cobra.Command) {
	cmd.Flags().Int64(keyMaxSize, 0, "max size in bytes")
}

func useFlagWarnSize(cmd *cobra.Command) {
	cmd.Flags().Int64(keyWarnSize, 0, "warn size in bytes")
}

func useFlagWriter(cmd *cobra.Command) {
	cmd.Flags().String(keyWriter, "", "writer name")
}

func useFlagRowsPerMinute(cmd *cobra.Command) {
	cmd.Flags().Int64(keyRowsPerMinute, 0, "rows per minute")
}

func useFlagDataDogAddress(cmd *cobra.Command) {
	cmd.Flags().String(keyDataDogAddress, "", "address of the dogstatsd agent")
}

func useFlagDataDogBufferSize(cmd *cobra.Command) {
	cmd.Flags().Int(keyDataDogBufferSize, 0, "buffer size for the dogstatsd client")
}

func useFlagDataDogTags(cmd *cobra.Command) {
	cmd.Flags().StringSlice(keyDataDogTags, []string{}, `list of tags to use to add to datadog metrics. format: ["key:value","key:value"]`)
}

func useFlagCTLDBAddress(cmd *cobra.Command) {
	cmd.Flags().String(keyCTLDBAddress, "", "address of the ctldb")
}

func useFlagSoRAddress(cmd *cobra.Command) {
	cmd.Flags().String(keySoRAddress, "", "address of the SoR")
}
