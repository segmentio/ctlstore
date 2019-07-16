import React from "react";
import { ListItem, OrderedList, Pane, Heading, Paragraph } from "evergreen-ui";
import Highlight from "react-highlight"

export default function Writers() {
  return (
    <Pane>
      <Heading is="h1" size={900} textAlign="center" marginBottom={32}>
        Writers
      </Heading>
      <Paragraph size={500} marginBottom={32}>
        All ctlstore mutations must happen through the executive HTTP API. Programs that mutate through this API
          are known as writers. Most of the time the writer will be taking control from external SoRs
          (application databases) and converting those to executive mutations.
          Before that can happen, though, a little bit of setup is required (each step is an executive endpoint):
      </Paragraph>
        <Paragraph size={500} marginBottom={32}>
            1. Create the table family
            <Highlight>
                {`
$ curl -v -XPOST http://ctlstore-executive/families/myfamily                
                `}
            </Highlight>
        </Paragraph>
        <Paragraph size={500} marginBottom={32}>
            2. Create the table with a schema
            <Highlight>
                {`

curl -XPOST -d @- http://ctlstore-executive/families/myfamily/tables/mytable <<EOF
{
    "fields": [
        ["field_1", "string"],
        ["field_2", "binary"]
    ],
    "keyFields": ["field_1"]
}
EOF
                `}
            </Highlight>
        </Paragraph>
        <Paragraph size={500} marginBottom={32}>
            3. Register the writer with ctlstore (each writer has a name and a secret)
            <Highlight>
                {`
$ curl -XPOST -d "SECRET" http://ctlstore-executive/writers/my-writer                
                `}
            </Highlight>
        </Paragraph>
        <Paragraph size={500} marginBottom={32}>
            4. You can now send mutations using those writer creds
            <Highlight>
                {`
curl -d @- http://ctlstore-executive/families/myfamily/mutations <<EOF
{
  "mutations": [
    {
      "table": "mytable",
      "delete": false,
      "values": {
        "field_1": "foo",
        "field_2": "Zm9vYmFy"
      }
    }  
  ],
}
EOF
`}
            </Highlight>
        </Paragraph>
    </Pane>
  )
}
