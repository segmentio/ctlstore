import React from "react";
import { HashRouter as Router, Route } from "react-router-dom";
import { Pane } from "evergreen-ui";
import Nav from "./Nav";
import Content from "./Content";
import GettingStarted from "./GettingStarted";

function App() {
  return (
    <Router>
      <Pane minHeight="100vh">
        <Nav />
        <Route path="/" exact component={Content} />
        <Route path="/get-started" component={GettingStarted} />
        <Route component={Pane} />
      </Pane>
    </Router>
  );
}

export default App;
