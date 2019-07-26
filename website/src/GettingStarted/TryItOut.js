import React from "react";
import { ListItem, UnorderedList, Pane, Heading, Paragraph } from "evergreen-ui";
import Highlight from "react-highlight"

window.analytics.page("Getting Started -- Try It Out", {
    title: "Getting Started -- Try It Out"
});

export default function TryItOut() {
  return (
    <Pane>
      <Heading is="h1" size={900} textAlign="center" marginBottom={32}>
        Try It Out
      </Heading>
      <Paragraph size={500} marginBottom={32}>
        The ctlstore repo sports a docker compose environment that puts all of these components we’ve discussed (and more) into action. To stand up the environment:
        <Highlight>
          {`$ docker-compose -f docker-compose-example.yml up -d`}
        </Highlight>
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
        The components it starts:
        <UnorderedList size={500}>
          <ListItem>executive: guards write access to the ctldb</ListItem>
          <ListItem>supervisor: periodically creates a backup of the LDB on disk</ListItem>
          <ListItem>mysql: this is the SoR for ctlstore (in production this would live in Aurora)</ListItem>
          <ListItem>reflector: writes to the LDB on a shared volume</ListItem>
          <ListItem>sidecar: exposes the ctlstore read API over HTTP (useful for services which are not written in Go)</ListItem>
          <ListItem>heartbeat: a program that mutates a heartbeats table periodically via the executive API</ListItem>
        </UnorderedList>
      </Paragraph>
        <Paragraph>
            Eventually you should see the heartbeats show up in the local database:
            <Highlight>
                {`
$ docker-compose -f docker-compose-example.yml exec reflector sqlite3 /var/spool/ctlstore/ldb.db

SQLite version 3.28.0 2019-04-16 19:49:53
Enter ".help" for usage hints.
sqlite> select * from ctlstore___heartbeats;
heartbeat|1563208201658223400
sqlite>
                `}
            </Highlight>
        </Paragraph>
    </Pane>
  )
}
