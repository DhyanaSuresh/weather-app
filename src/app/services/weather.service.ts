import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Observable, throwError, map, catchError, retry, of } from 'rxjs';
import { environment } from '../../environments/environment.development';
import { WeatherResponse, WeatherDisplay } from '../models/weather.model';

//caching
interface CacheEntry {
  data: WeatherDisplay;
  timestamp: number;
}

@Injectable({ //Angluar creats one single instance of WeatherService for the entire appln.
  providedIn: 'root' //root means available everywhere
})
export class WeatherService {

  //for local storage
  private readonly HISTORY_KEY = 'weather_history';
  private readonly MAX_HISTORY = 10;

  private apiUrl = environment.weatherApiUrl;
  private apiKey = environment.weatherApiKey;

  private cache = new Map<string, CacheEntry>();
  private readonly CACHE_TTL = 10 * 60 * 1000;

  constructor(private http: HttpClient) {}

  getWeather(city: string): Observable<WeatherDisplay> { //The method returns an Observable (not a value). 
    const key = city.toLowerCase();
    const cached = this.cache.get(key);
    const now = Date.now();

    // return cached if valid
    if (cached && (now - cached.timestamp) < this.CACHE_TTL) {
      return of(cached.data);
    }

    const url = `${this.apiUrl}/weather?q=${city}&appid=${this.apiKey}&units=metric`;

    return this.http.get<WeatherResponse>(url).pipe( //pipe() is an RxJS method that lets you chain operators. 
      retry(1),
      map(raw => { //map() transforms each emitted value - here it converts the raw WeatherResponse into our cleaner WeatherDisplay. 
        const display = this.mapToDisplay(raw);
        this.cache.set(key, { data: display, timestamp: now });
        return display;
      }),
      catchError(this.handleError)
    ); 
  } 

  private handleError(error: HttpErrorResponse): Observable<never> {
    let message: string;

    if (error.status === 0) {
      message = 'No internet connection. Please check your network.';
    } else if (error.status === 401) {
      message = 'Invalid API key. Please check your configuration.';
    } else if (error.status === 404) {
      message = 'City not found. Please check the spelling and try again.';
    } else if (error.status === 429) {
      message = 'Too many requests. Please wait a moment and try again.';
    } else {
      message = `Server error (code ${error.status}). Please try again later.`;
    }

    return throwError(() => new Error(message));
  }

  private mapToDisplay(raw: WeatherResponse): WeatherDisplay {
    return {
      city: raw.name,
      country: raw.sys.country,
      temperature: Math.round(raw.main.temp),
      feelsLike: Math.round(raw.main.feels_like),
      description: raw.weather[0].description,
      humidity: raw.main.humidity,
      windSpeed: Math.round(raw.wind.speed * 3.6),
      icon: raw.weather[0].icon,
      tempMin: Math.round(raw.main.temp_min),
      tempMax: Math.round(raw.main.temp_max)
    };
  }
  
  //local storage functions below - get, add, clear
  getSearchHistory(): string[] {
    const raw = localStorage.getItem(this.HISTORY_KEY);
    return raw ? JSON.parse(raw) : [];
  }

  addToHistory(city: string): void {
    let history = this.getSearchHistory();
    history = history.filter(c => c.toLowerCase() !== city.toLowerCase()); // Remove duplicate if exists
    history.unshift(city); // Add to front
    history = history.slice(0, this.MAX_HISTORY); // Keep max 10 entries
    localStorage.setItem(this.HISTORY_KEY, JSON.stringify(history));
  }

  clearHistory(): void {
    localStorage.removeItem(this.HISTORY_KEY);
  }
}