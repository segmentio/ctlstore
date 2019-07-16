import React from 'react'
import { Pane, Tablist, SidebarTab } from 'evergreen-ui'
import { Link as RouterLink, Switch, Redirect, Route } from 'react-router-dom'
import Introduction from './Introduction'
import Concepts from './Concepts'
import TryItOut from './TryItOut'
import Tables from './Tables'
import Writers from './Writers'
import Readers from './Readers'
import Sidecar from './Sidecar'
import Tests from './Tests'

const routes = [
  {
    label: 'Introduction',
    path: '/introduction',
    component: Introduction
  },
  {
    label: 'Concepts',
    path: '/concepts',
    component: Concepts
  },
  {
    label: 'Try It Out',
    path: '/try-it-out',
    component: TryItOut
  },
  {
    label: 'Tables',
    path: '/tables',
    component: Tables
  },
  {
    label: 'Writers',
    path: '/writers',
    component: Writers
  },
  {
    label: 'Readers',
    path: '/readers',
    component: Readers
  },
  {
    label: 'Sidecar',
    path: '/sidecar',
    component: Sidecar
  },
  {
    label: 'Tests',
    path: '/tests',
    component: Tests
  }
]

function Sidebar({ match }) {
  const path = window.location.pathname
  return (
    <Tablist
      flex="0 0 200px"
      paddingY={32}
      marginBottom={16}
      marginRight={24}
    >
      {routes.map(route => (
        <SidebarTab
          size={400}
          id={route.label}
          is={RouterLink}
          to={`${match.url}${route.path}`}
          isSelected={path === `${match.url}${route.path}`}
        >
          {route.label}
        </SidebarTab>
      ))}
    </Tablist>
  )
}

function GettingStarted({ match }) {
  return (
    <Pane display="flex" maxWidth={1080} marginX="auto" minHeight="calc(100vh - 57px)">
      <Sidebar match={match} />
      <Pane
        flex={1}
        paddingY={36}
        paddingX={48}
        borderLeft="muted"
      >  
        <Switch>
          {routes.map(route => (
            <Route
              key={route.label}
              exact
              path={`${match.path}${route.path}`}
              component={route.component}
            />
          ))}
          <Redirect to={`${match.path}/introduction`} />
        </Switch>
      </Pane>
    </Pane>
  )
}

export default GettingStarted