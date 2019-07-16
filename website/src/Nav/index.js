import React from "react";
import { Strong, Pane, Link, Text } from "evergreen-ui";
import { Link as RouterLink } from "react-router-dom";
import GitHubButton from "react-github-btn";

const Nav = () => (
  <Pane
    id="nav"
    maxWidth={1080}
    marginY="0"
    marginX="auto"
    paddingX={0}
    borderBottom="default"
    position="sticky"
    alignItems="center"
  >
    <Pane display="flex" alignItems="center" height="56px">
      <Pane flex="1" textTransform="uppercase">
        <RouterLink to="/" style={{ textDecoration: "none" }}>
          <Strong size={600} textTransform="none" marginRight={32}>
            ctlstore
          </Strong>
        </RouterLink>
        <RouterLink
          to="/get-started"
          style={{ textDecoration: "none", color: "#557490" }}
        >
          <Text marginRight={16} color="neutral" fontWeight={600} as={Link}>
            Get Started
          </Text>
        </RouterLink>
        <Link
          href="https://godoc.org/github.com/segmentio/ctlstore"
          marginRight={16}
          color="neutral"
          textDecoration="none"
          fontWeight={600}
        >
          go docs
        </Link>
        <Link
          href="https://github.com/segmentio/ctlstore"
          marginRight={16}
          color="neutral"
          textDecoration="none"
          fontWeight={600}
        >
          GitHub
        </Link>
      </Pane>
      <Pane>
        <GitHubButton
          href="https://github.com/segmentio/ctlstore"
          data-icon="octicon-star"
          data-size="large"
          data-show-count="true"
          aria-label="Star segmentio/ctlstore on GitHub"
        >
          Star
        </GitHubButton>
      </Pane>
    </Pane>
  </Pane>
);

export default Nav;
