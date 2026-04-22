//src/app/components/loading-spinner/loading-spinner.component.ts
import { Component, Input } from '@angular/core';
@Component({
 selector: 'app-loading-spinner',
 standalone: true,
 templateUrl: './loading-spinner.component.html',
 styleUrl: './loading-spinner.component.scss'
})
export class LoadingSpinnerComponent {
 @Input() message: string = 'Loading...'; //marks a property as an input, means the parent component can pass a value into this property from outside
}