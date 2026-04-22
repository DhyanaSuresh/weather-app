import { Component, OnInit, Output, EventEmitter } from '@angular/core'; //lifecycle hook interface that allows you to perform initialization logic
import { CommonModule } from '@angular/common';
import { WeatherService } from '../../services/weather.service';

@Component({
  selector: 'app-search-history', //selector this is the HTML tag we use to display this component
  standalone: true,
  imports: [CommonModule],
  templateUrl: './search-history.component.html',
  styleUrl: './search-history.component.scss'
})
export class SearchHistoryComponent implements OnInit {
  @Output() citySelected = new EventEmitter<string>();
  //When the user clicks a history item, this emits the city name (a string) to the parent
  //The parent can then trigger a search for that city.
  history: string[] = [];
  constructor(private weatherService: WeatherService) {}
  ngOnInit(): void { //A lifecycle hook method that Angular calls automatically once, right after the component is created and its inputs are set.
    this.loadHistory();
  }
  loadHistory(): void {
    this.history = this.weatherService.getSearchHistory(); //Component->calls->WeatherService->gets local storage data
  }
  onCityClick(city: string): void { //child talks to parent w this
    this.citySelected.emit(city);
  }
  onClear(): void {
    this.weatherService.clearHistory();
    this.history = [];
  }
}