import React from "react";
import { Pane, Heading, Paragraph } from "evergreen-ui";
import Highlight from "react-highlight"

export default function Readers() {
  return (
    <Pane>
      <Heading is="h1" size={900} textAlign="center" marginBottom={32}>
        Readers
      </Heading>
      <Paragraph size={500} marginBottom={32}>
        Go programs can use the github.com/segmentio/ctlstore package to read from the LDB. Sensible defaults allow a program to:
        <Highlight className="golang">
          {`reader, err := ctlstore.Reader()`}
        </Highlight>
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
        Data inside of the LDB can be unmarshaled into a struct via the use of special struct property tags:
        <Highlight className="golang">
          {`type AppRow struct {
  UserID string \`ctlstore:"user_id"\`
  PubKey string \`ctlstore:"public_key"\`
}`}
        </Highlight>
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
        The ctlstore tags instruct the reader how to map table columns to struct properties.
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
        Then the read is performed like this:
        <Highlight className="golang">
          {`var out AppRow
key := []interface{}{ "user@example.com" }
err := reader.GetRowByPrimaryKey(ctx, &out, "my-family", "my-table", key...)
`}
        </Highlight>
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
        Note that it’s important to not ignore error values returned from this API. The most common error occurs when the reader cannot find the LDB due to a misconfigured deploy (missing volume mount, etc).
      </Paragraph>
    </Pane>
  )
}
