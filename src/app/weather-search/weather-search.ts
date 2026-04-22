// src/app/weather-search/weather-search.ts
import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms'; //for two way data binding , w/o importing this ngModel wont work
import { CommonModule } from '@angular/common'; //provides common directives like ngIf , ngFor 
import { HttpClient } from '@angular/common/http';
import { environment } from '../../environments/environment.development';
import { WeatherService } from '../services/weather.service';
import { WeatherResponse, WeatherDisplay } from '../models/weather.model';
import { ErrorMessageComponent } from '../components/error-message/error-message.component';
import { LoadingSpinnerComponent } from '../components/loading-spinner/loading-spinner.component';
import { WindSpeedPipe } from '../pipes/wind-speed.pipe';
import { ChangeDetectorRef } from '@angular/core'; // ✅ ADDED

// Angular Material UI components
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatChipsModule } from '@angular/material/chips';
import { MatIconModule } from '@angular/material/icon';
import { MatFormFieldModule } from '@angular/material/form-field';

@Component({
  selector: 'app-weather-search',
  standalone: true,
  imports: [
    FormsModule,
    CommonModule,
    LoadingSpinnerComponent,
    ErrorMessageComponent,
    WindSpeedPipe,

    // Angular Material modules 👇
    MatInputModule, //Input fields
    MatButtonModule, //Buttons
    MatCardModule, //Cards (weather UI box)
    MatProgressSpinnerModule, //Loading spinner
    MatChipsModule, //Chips (tags, history UI)
    MatIconModule, //Icons
    MatFormFieldModule //Wrapper for inputs
],  
  templateUrl: './weather-search.html',
  styleUrl: './weather-search.scss'
})

export class WeatherSearchComponent{

  cityName: string = '';
  lastCity: string = '';   
  hasSearched: boolean = false;
  isLoading: boolean = false;
  errorMessage: string = '';
  weatherData: WeatherDisplay | null = null; //The | null is a union type in TypeScript. can either be obj. or null

  //Inject weather service instead of HTTpClient
constructor(private weatherService: WeatherService,
            private cdr: ChangeDetectorRef) {} // ✅ ADDED

  // constructor(private http: HttpClient) {}  //This is Dependency Injection (DI) in action. 
                                              // Instead of creating an HttpClient yourself (new HttpClient()), you declare that you need one as a constructor parameter, and Angular's DI system provides it automatically.

  onSearch(): void {
    const city = this.cityName.trim();
    if (!city) return;

    this.isLoading = true;
    this.errorMessage = '';
    this.hasSearched = true;
    this.cityName = '';
    this.lastCity = city; 

    // const url = `${environment.weatherApiUrl}/weather?q=${city}&appid=${environment.weatherApiKey}&units=metric`; //We build the API URL dynamically inserting the city name and API key

    // this.http.get<WeatherResponse>(url).subscribe({ //Makes an HTTP GET request to the URL, <WeatherResponse> tells the type for type safety
      //.subscribe({}) : Executes the HTTP request and handles results

      this.weatherService.getWeather(city).subscribe({
        next: (data) => {
          this.weatherData = data;
          this.isLoading = false;
          this.cdr.detectChanges(); // CDR for immediate loading
        },
        error: (err) => {
          this.errorMessage = err.status === 404
            ? 'City not found. Please check the spelling.'
            : 'Something went wrong. Please try again.';
          this.isLoading = false;
          this.weatherData = null;
          this.cdr.detectChanges(); // CDR for immediate loading
        }
      });
  }
 
 // private mapToDisplay(raw: WeatherResponse): WeatherDisplay { //This has been changed to WEATER.SERVICE.TS
  //  return {
    //  city: raw.name,
    //  country: raw.sys.country,
    //  temperature: Math.round(raw.main.temp), 
    //  feelsLike: Math.round(raw.main.feels_like),
    //  description: raw.weather[0].description,
    //  humidity: raw.main.humidity,
    //  windSpeed: Math.round(raw.wind.speed * 3.6), //OpenWeatherMap returns wind speed in meters/second.
    //                                              //Multiplying by 3.6 converts to km/h. Math.round() removes decimal points for cleaner display. 
    //  icon: raw.weather[0].icon,
    //  tempMin: Math.round(raw.main.temp_min),
    //  tempMax: Math.round(raw.main.temp_max)
  // };
// }
  onRetry(): void {
    this.errorMessage = '';
    this.onSearch();
  }

  onKeyPress(event: KeyboardEvent): void {
    if (event.key === 'Enter') {
      this.onSearch();
    }
  }
}