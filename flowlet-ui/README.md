# Flowlet UI

A modern web UI for the Flowlet workflow orchestration system, built with Angular 20.2 and Bootstrap 5.

## Overview

Flowlet UI provides a user-friendly interface for managing and monitoring workflow executions. It connects to the Flowlet API backend to:

- View and manage available workflows
- Trigger flow executions
- Monitor flow runs and their status
- View detailed task information for each run

## Prerequisites

- Node.js (v18 or higher)
- npm (v9 or higher)
- Flowlet API backend running on `http://localhost:8000`

## Installation

1. Install dependencies:

```bash
npm install
```

## Development Server

To start a local development server, run:

```bash
npm start
# or
ng serve
```

Once the server is running, open your browser and navigate to `http://localhost:4200/`. The application will automatically reload whenever you modify any of the source files.

## Configuration

### API Proxy

The application uses a proxy configuration for development to avoid CORS issues. The proxy is configured in [proxy.conf.json](proxy.conf.json) and routes requests from `/api` to `http://localhost:8000`.

If you need to connect to a different backend URL, update the target in `proxy.conf.json`.

### Backend CORS Setup

If you want to run without the proxy, you'll need to enable CORS in the Flowlet API backend. Add the following to your backend:

```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

Then update the API URL in [src/app/services/flowlet-api.ts](src/app/services/flowlet-api.ts#L43) to point directly to your backend.

## Code scaffolding

Angular CLI includes powerful code scaffolding tools. To generate a new component, run:

```bash
ng generate component component-name
```

For a complete list of available schematics (such as `components`, `directives`, or `pipes`), run:

```bash
ng generate --help
```

## Building

To build the project run:

```bash
ng build
```

This will compile your project and store the build artifacts in the `dist/` directory. By default, the production build optimizes your application for performance and speed.

## Running unit tests

To execute unit tests with the [Karma](https://karma-runner.github.io) test runner, use the following command:

```bash
ng test
```

## Running end-to-end tests

For end-to-end (e2e) testing, run:

```bash
ng e2e
```

Angular CLI does not come with an end-to-end testing framework by default. You can choose one that suits your needs.

## Additional Resources

For more information on using the Angular CLI, including detailed command references, visit the [Angular CLI Overview and Command Reference](https://angular.dev/tools/cli) page.
