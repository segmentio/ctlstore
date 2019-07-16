import React from "react";
import { Pane, Heading, Paragraph } from "evergreen-ui";
import Highlight from "react-highlight"

export default function Sidecar() {
  return (
    <Pane>
      <Heading is="h1" size={900} textAlign="center" marginBottom={32}>
        Sidecar
      </Heading>
      <Paragraph size={500} marginBottom={32}>
        Not all data plane components are written in Go, though, and to support these programs it’s recommended to use the ctlstore sidecar. The sidecar is a very simple Go program that composes an LDB Reader and maps each method of its API to an HTTP endpoint. Any programming environment that can speak HTTP can then read from ctlstore using the sidecar.
      </Paragraph>
        <Paragraph size={500} marginBottom={32}>
            The following example shows how the sidecar might be used to fetch a row of data:

            <Highlight className="golang">
                {`curl -d '{"key": [{"value":"heartbeat"}]}}' \\
    http://ctlstore-sidecar/get-row-by-key/ctlstore/heartbeats
                
{
    "name": "heartbeat",
    "value": 1563217544067280400
}
`}
            </Highlight>

        </Paragraph>
    </Pane>
  )
}
