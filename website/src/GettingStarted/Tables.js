import React from "react";
import { Pane, Heading, Paragraph } from "evergreen-ui";
import Highlight from "react-highlight"

window.analytics.page("Getting Started -- Tables", {
    title: "Getting Started -- Tables"
});

export default function Tables() {
  return (
    <Pane>
      <Heading is="h1" size={900} textAlign="center" marginBottom={32}>
        Tables
      </Heading>
      <Paragraph size={500} marginBottom={32}>
        Ctlstore is a RDBMS. All control data in the system is stored in tables. Part of the multi-tenant design introduces the concept of a table family. The family is effectively a namespace for tables. Because there is one central SoR, the family concept helps to prevent table name collisions. Thus, when data is either read or written, it is required that both the family and the table name be specified.
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
        In the ctldb, the tables that contain the mutations are named using the format 
        <Highlight>
          {`$\{family}___$\{table}`}
        </Highlight>
      </Paragraph>
      <Paragraph color="muted" size={500} marginBottom={32} fontStyle="italic">
        Note that the readers will not query the database directory. They will use the reader library instead, which hides this complexity and also enforces certain access patterns.
      </Paragraph>
    </Pane>
  )
}
