// src/app/pipes/wind-speed.pipe.ts
import { Pipe, PipeTransform } from '@angular/core';

@Pipe({  //declares this class as pipe 
  name: 'windSpeed', // Angular looks up this pipe by name - windSpeed
  standalone: true
})

export class WindSpeedPipe implements PipeTransform { //interface that requires you to implement transform()
  transform(value: number, unit: string = 'km/h'): string { //value is the number before the pipe , unit is an optional argument after a colon
    if (value < 15) return `${value} ${unit} (Calm)`;
    if (value < 30) return `${value} ${unit} (Moderate)`;
    if (value < 70) return `${value} ${unit} (Strong)`;
    return `${value} ${unit} (Very Strong)`;
  }
}