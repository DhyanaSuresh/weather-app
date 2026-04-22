// src/environments/environment.ts
export const environment = {
 production: true, //A flag that tells the app whether it is running in dev or prod mode.
                  //Angular automatically uses environment.development.ts during ng serve and environment.ts during ng build.
 weatherApiKey: '947b61f3efe5ef3b0b06227c7ff14cec',
 weatherApiUrl: 'https://api.openweathermap.org/data/2.5'
};