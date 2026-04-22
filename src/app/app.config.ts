// src/app/app.config.ts
import { ApplicationConfig } from '@angular/core';
import { provideRouter } from '@angular/router';
import { provideHttpClient } from '@angular/common/http'; //switching ON http feature of whole app ,  Registers Angular's HTTP client with the dependency injection system
import { routes } from './app.routes';
export const appConfig: ApplicationConfig = {
 providers: [
 provideRouter(routes),
 provideHttpClient()
 ]
};
