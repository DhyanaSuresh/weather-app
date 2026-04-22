# WeatherApp — Angular

A weather application built with Angular 17 and Angular Material, featuring real-time weather data from the OpenWeatherMap API, an SVG-based wind animation layer, search history, and a clean glass-morphism UI.

---

## Preview

> Search any city to get live weather — temperature, humidity, wind speed with animated wind streaks, and more.

---

## Tech Stack

| Layer | Tech |
|-------|------|
| Framework | Angular 17 |
| UI Library | Angular Material |
| Styling | SCSS, Glass-morphism |
| API | OpenWeatherMap |
| Deployment | GitHub Pages |

---

## Project Structure

```
weather-app/
└── src/
    ├── app/
    │   ├── components/         # Reusable UI — loading spinner, error message
    │   ├── models/             # TypeScript interfaces (WeatherData)
    │   ├── pipes/              # Custom windSpeed pipe
    │   ├── services/           # WeatherService (HttpClient + RxJS)
    │   └── weather-search/     # Main feature component
    │       ├── weather-search.component.ts
    │       ├── weather-search.component.html
    │       └── weather-search.component.scss
    └── environments/
        ├── environment.ts
        └── environment.development.ts
```

---

## Features

- City search with live weather data from OpenWeatherMap
- Displays temperature, feels-like, min/max, humidity, and wind speed
- **SVG wind animation layer** — 13 curved SVG path elements rendered directly on the weather card, animating via `stroke-dashoffset` to simulate actual wind movement. Wind intensity is driven by the live `windSpeed` value and mapped to four CSS classes (`wind-calm`, `wind-moderate`, `wind-strong`, `wind-extreme`), each adjusting stroke opacity, width, and animation duration. The streaks are always visible even at low wind speeds, with an edge-fade gradient mask so they dissolve naturally at the card boundaries.
- Centralized error handling with retry support
- Loading spinner component
- Search history persisted via localStorage
- Angular Material components — `mat-card`, `mat-chip`, `mat-progress-spinner`, `mat-icon`

---

## Getting Started

### Prerequisites

- Node.js `v18+`
- Angular CLI — `npm install -g @angular/cli`
- An [OpenWeatherMap API key](https://openweathermap.org/api) (free tier)

### Installation

```bash
git clone https://github.com/DhyanaSuresh/weather-app.git
cd weather-app
npm install
```

### Configuration

In `src/environments/environment.development.ts`, add your API key:

```ts
export const environment = {
  production: false,
  weatherApiUrl: 'https://api.openweathermap.org/data/2.5',
  weatherApiKey: 'your_api_key_here',
};
```

> Note: This exposes your key in the browser's Network tab during development. For production, route requests through a backend proxy and remove the key from the frontend entirely.

### Running Locally

```bash
ng serve
```

App runs at `http://localhost:4200`

---

## Deployment

Deployed to GitHub Pages via `angular-cli-ghpages`:

```bash
ng build --base-href "https://DhyanaSuresh.github.io/weather-app/"
npx angular-cli-ghpages --dir=dist/weather-app
```

---

## Angular Concepts Covered

This project was built as a structured learning exercise. Concepts implemented across phases:

- Components, Modules, and project architecture
- Two-way data binding, property binding, event binding
- Structural directives — `*ngIf`, `*ngFor`
- Services and Dependency Injection
- `HttpClient` with RxJS Observables
- `catchError` and `retry` operators
- Custom Pipe (`windSpeed`)
- `@Input()` and `@Output()` with `EventEmitter`
- Environment-based configuration
- localStorage for persistent state
- SVG animations with CSS custom properties and keyframes
