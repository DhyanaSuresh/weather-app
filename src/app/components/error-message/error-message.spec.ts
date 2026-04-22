// src/app/components/error-message/error-message.component.ts
import { Component, Input, Output, EventEmitter } from '@angular/core';
@Component({
  selector: 'app-error-message',
  standalone: true,
  templateUrl: './error-message.component.html',
  styleUrl: './error-message.component.css'
})
export class ErrorMessageComponent {
  @Input() message: string = 'An error occurred.';
  @Output() retry = new EventEmitter<void>(); //marks a property as an output , the child component can emit events that the parents listen to
  onRetry(): void {
  this.retry.emit(); //The parent component listening with (retry)="onRetry()" will have its onRetry() method called.
  }
}