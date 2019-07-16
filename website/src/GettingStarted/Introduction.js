import React from "react";
import { Link, Pane, Heading, Paragraph } from "evergreen-ui";

export default function Introduction() {
  return (
    <Pane>
      <Heading is="h1" size={900} textAlign="center" marginBottom={32}>
        Introduction
      </Heading>
      <Paragraph size={500} marginBottom={32}>
        Ctlstore is a distributed, multi-tenant data store that replicates its data from a central system of record (SoR) to a local database (LDB) that lives on each server in the fleet. Its design enables data plane components to withstand infrastructure downtime due to those components reading control data from the LDB instead of an external database.
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
        Because the read path is always local, it boasts fast access times (typically in the microsecond range). If the SoR becomes unavailable, the data plane components will continue to process data because they are reading from their local copy; when the SoR becomes available, the replication process will resume.
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
          Ctlstore is meant to enable reliable access to control data. This <Link size={500} href="https://segment.com/blog/separating-our-data-and-control-planes-with-ctlstore/">blog post</Link> goes into detail about ctlstore, some of which will also live in this guide. If you’re curious why we built this, Segment co-founder Calvin French-Owen <Link size={500} href="https://vimeo.com/293246627">spoke about our motivations for building ctlstore</Link> at Synapse 2018.
      </Paragraph>
      <Paragraph size={500} marginBottom={32}>
          Finally, we wanted to share ctlstore with the community as soon as possible.  As time goes on we hope to
          expand this guide, add architecture diagrams, improve the documentation in general, add API references,
          improve the site experience on mobile, and so on.  Thanks for your patience!
      </Paragraph>
    </Pane>
  )
}
