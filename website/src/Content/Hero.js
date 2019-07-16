import React from "react";
import Highlight from "react-highlight";
import { Pane, Card, Heading, defaultTheme } from "evergreen-ui";

const background = `
linear-gradient(-180deg,
  ${defaultTheme.palette.green.lightest} 20%,
  ${defaultTheme.palette.green.light} 30%,
  ${defaultTheme.palette.green.light} 80%
)
`;

const Hero = () => (
  <Pane
    id="hero"
    display="flex"
    alignItems="center"
    justifyContent="center"
    flexDirection="column"
    marginY={32}
    background={background}
  >
    <Heading size={900} fontWeight={600} marginBottom={8}>
      ctlstore
    </Heading>
    <Heading size={600} marginBottom={32}>
      Push your control data to the edge.
    </Heading>
    <Terminal />
    <Pane width={700} id="golang">
      <Highlight>
        {`
// data plane service reads config value from local DB
func handleMessage(ctx context.Context, msg Message) error {
  type Options struct {
    CustomerID string \`ctlstore:"customer_id"\`
    EnableXYZ  int64  \`ctlstore:"enable_xyz"\`
  }

  var reader := ctlstore.Reader()
  var opts Options
  
  customer = msg.CustomerID // e.g. "f9xirj2ldkdjfs" 
  found, err := reader.GetRowByKey(ctx, &opts, "config", "opts", customer)
  if err != nil {
    return err
  }
  if found && opts.EnableXYZ {
    // feature XYZ is on for the customer
  }
}

`}
      </Highlight>
    </Pane>
  </Pane>
);

const Terminal = () => (
  <Card width={700} backgroundColor="#425A70" elevation={0}>
    <Pane
      height={30}
      backgroundColor="#66788A"
      borderTopLeftRadius={5}
      borderTopRightRadius={5}
      display="flex"
      alignItems="center"
    >
      <Pane
        backgroundColor="#425A70"
        height={15}
        width={15}
        borderRadius="50%"
        marginLeft={10}
      />
      <Pane
        backgroundColor="#425A70"
        height={15}
        width={15}
        borderRadius="50%"
        marginX={5}
      />
      <Pane
        backgroundColor="#425A70"
        height={15}
        width={15}
        borderRadius="50%"
      />
    </Pane>
    <Pane paddingX={12} id="terminal">
      <Highlight>
        {`

# upsert a row in the opts table for a customer
curl -d @- http://ctlstore-executive/families/config/mutations <<EOF
{
  "mutations": [
    {
      "table": "opts",
      "delete": false,
      "values": {
        "customer_id": "f9xirj2ldkdjfs",
        "enable_xyz": 1
      }
    }  
  ],
}
EOF
`}
      </Highlight>
    </Pane>
  </Card>
);

export default Hero;
