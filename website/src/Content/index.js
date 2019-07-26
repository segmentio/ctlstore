import React from "react";
import { Pane, Heading, Text, defaultTheme } from "evergreen-ui";
import Highlight from "react-highlight";
import Hero from "./Hero";

window.analytics.page("Home Page", {
  title: "Home Page"
});

const Content = () => (
  <Pane>
    <Hero />
    <Pane
      display="flex"
      justifyContent="center"
      textAlign="center"
      background={defaultTheme.palette.neutral.dark}
    >
      <Pane
        width={600}
        display="flex"
        alignItems="center"
        justifyContent="center"
        flexDirection="column"
        marginRight={36}
      >
        <Heading size={900} color="white" marginBottom={16}>
          Your app shouldn't crash when your database falls over
        </Heading>
        <Text color="white" size={500}>
          ctlstore replicates data from a central system of record to a local
          sqlite database that lives on the same machines as your app.  Your app
          can query the local database even if your system of record goes down.
        </Text>
      </Pane>
      <Pane textAlign="left" marginTop={36}>
        <Highlight>
          {`
  package store

  import "github.com/segmentio/ctlstore"

  type MetadataRow struct {
    Key string \`ctlstore:"key"\`
    Val string \`ctlstore:"val"\`
  }

  // FindMetadata fetches metadata from the local db
  func (db DB) FindMetadata(ctx context.Context) (*MetadataRow, error) {
    var val MetadataRow
    reader := ctlstore.Reader()
    meta := new(MetadataRow)
    _, err := reader.GetRowByKey(ctx, meta, "resources", "metadata", "rs_123")
    return meta, err 
  }

                `}
        </Highlight>
      </Pane>
    </Pane>
    <Pane
      display="flex"
      justifyContent="center"
      textAlign="center"
      background={defaultTheme.palette.neutral.dark}
    >
      <Pane textAlign="left" marginTop={20}>
        <Highlight>
          {`
  package flags

  import "github.com/segmentio/ctlstore"

  type FlagRow struct {
    Name    string \`ctlstore:"flag_name"\`
    Users   []byte \`ctlstore:"enabled_users"\`
  }

  func Render(ctx context.Content, userID string) error {
    reader := ctlstore.Reader()
    row := new(FlagRow) 
    ok, err := reader.GetRowByKey(ctx, row, "config", "flags", "new_ui")
    if err != nil {
      return err
    }
    newUIEnabled := false
    if ok {
        m := make(map[string]bool)
        json.Unmarshal(&m, row.Users)
        if m[userID] {
          newUIEnabled = true
        }
    } 
    // render based on newUIEnabled
  }
                `}
        </Highlight>
      </Pane>
      <Pane
        width={600}
        display="flex"
        alignItems="center"
        justifyContent="center"
        flexDirection="column"
        marginLeft={36}
      >
        <Heading size={900} color="white" marginBottom={20}>
          Use your ctlstore data to drive logic and flow.
        </Heading>
        <Text color="white" marginBottom={20} size={500}>
          ctlstore is a great fit for control data like feature flags.
          Your app can query for this data to control business logic
          without reaching out to a remote database.
        </Text>
      </Pane>
    </Pane>
  </Pane>
);

export default Content;
