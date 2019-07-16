import React from "react";
import { Pane, Heading, Paragraph } from "evergreen-ui";

export default function Concepts() {
  return (
    <Pane>
      <Heading is="h1" size={900} textAlign="center" marginBottom={32}>
        Concepts
      </Heading>
      <Paragraph size={500} marginBottom={32}>
        The ctlstore data lives in a SoR called the ctldb (control database). The ctldb is an AWS Aurora database.
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
        Writes to the ctldb are guarded by a service called the executive. The executive’s sole responsibility is to expose an HTTP API that allows writers to mutate the ctldb in a controlled fashion. Each mutation performed through this API mutates the table specified and then appends generated SQL into a special table called the ledger. The ledger is an ordered list of mutations that are used to build the LDB on each container instance.
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
        Each container instance runs a program called the reflector. The purpose of the reflector is to continually poll the ctldb ledger and, apply updates from the ledger to the LDB. This LDB is what data plane components will query during normal operation for control data. 
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
        Each ledger entry in the ctldb has a unique sequence number. As each ledger entry is applied to the LDB, the sequence from that entry is transactionally set in a special metadata table. Because the LDB stores its position in the ledger, the reflector can easily restart after a crash. 
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
        There is an additional component called the supervisor. The supervisor continually builds its own LDB, much like a reflector, but it periodically pushes its LDB to S3. This LDB in S3 is a ctlstore snapshot. Because the ctldb ledger will have a larger number of rows, new container instances would take too long to get their LDBs up to date if they were to consume the ledger from the beginning; they instead query S3 to get the most recent snapshot and start from the ledger position recorded in the snapshot LDB. This allows new container instances to become up to date in minutes versus hours.
      </Paragraph>
    </Pane>
  )
}
