import React from "react";
import { Pane, Heading, Paragraph } from "evergreen-ui";
import Highlight from "react-highlight"

export default function Tests() {
  return (
    <Pane>
      <Heading is="h1" size={900} textAlign="center" marginBottom={32}>
        Tests
      </Heading>
      <Paragraph size={500} marginBottom={32}>
        To run the test suite, it’s required to first spin up a mysql database container:
        <Highlight>
          {`$ docker-compose up -d mysql`}
        </Highlight>
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
        Afterwards, you can run the tests by:
        <Highlight>
          {`$ make test`}
        </Highlight>
      </Paragraph>
    </Pane>
  )
}
