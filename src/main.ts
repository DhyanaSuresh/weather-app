import { bootstrapApplication } from '@angular/platform-browser'; //imports the fun. that starts angular
import { appConfig } from './app/app.config'; //imports the config. obj (providers,routing,etc.)
import { AppComponent } from './app/app.component'; //imports the root component 

bootstrapApplication(AppComponent, appConfig) //this is the ignition key.. it tells the angular to start w app using that config - appConfig
  .catch((err) => console.error(err)); //if startup fails then this gets printed 
