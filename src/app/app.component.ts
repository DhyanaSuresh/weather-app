import { Component } from '@angular/core'; //imports component decorator from angular library
import { RouterOutlet } from '@angular/router';
import { WeatherSearchComponent } from './weather-search/weather-search';

@Component({
  selector: 'app-root', //html tag name of this component
  standalone: true, //this means components manage its own imports rather than relying on shared NgModule
  imports: [RouterOutlet, WeatherSearchComponent ], //since its standalone, angular must explicitly list the angular features it uses 
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent { //makes it importable for other files
  title = 'weather-app'; //variable that belongs to component 
   currentYear = new Date().getFullYear(); //newDate creates JS data obj for right now, fullYear will give 4 digit yeat format
}