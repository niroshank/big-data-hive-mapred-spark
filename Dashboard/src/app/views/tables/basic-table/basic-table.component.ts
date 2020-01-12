import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-basic-table',
  templateUrl: './basic-table.component.html',
  styleUrls: ['./basic-table.component.scss']
})
export class BasicTableComponent implements OnInit {
  public map: any = { lat: 51.678418, lng: 7.809007 };
  public chart1Type:string = 'bar';
  public chart2Type:string = 'pie';
  public chart3Type:string = 'line';
  public chart4Type:string = 'radar';
  public chart5Type:string = 'doughnut';


  public chartType = 'line';

  //Q2 data
  public chartDatasets1: Array<any> = [
    {data: [50], label: 'Central Region'},
    {data: [28], label: 'East Region'},
    {data: [38], label: 'North Region'},
    {data: [38], label: 'North-East Region'},
    {data: [38], label: 'West Region'}
  ];

   //Q2 data
  public chartDatasets2: Array<any> = [
    {data: [50], label: 'Central Region'},
    {data: [28], label: 'East Region'},
    {data: [38], label: 'North Region'},
    {data: [38], label: 'North-East Region'},
    {data: [38], label: 'West Region'}
  ];
  
  public chartColors:Array<any> = [];
  public dateOptionsSelect: any[];
  public bulkOptionsSelect: any[];
  public showOnlyOptionsSelect: any[];
  public filterOptionsSelect: any[];
  public chartOptions: any = {
    responsive: true,
    legend: {
      labels: {
        fontColor: '#5b5f62',
      }
    },
    scales: {
      yAxes: [{
        ticks: {
          fontColor: '#5b5f62',
        }
      }],
      xAxes: [{
        ticks: {
          fontColor: '#5b5f62',
        }
      }]
    }
  };
  constructor() { }

  ngOnInit() {
  }

}
