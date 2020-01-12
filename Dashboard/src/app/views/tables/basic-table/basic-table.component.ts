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
    {data: [27.54,72.46], label: 'Rent more than one property'}
  ];

  public chartLabels: Array<any> = ['Rent more than one property', 'Rent one property'];
   //Q2 data
  public chartDatasets2: Array<any> = [
    {data: [1,1,1,1,3,1,2,6,3,2,4,6,10,10,9,9,14,7,3,3,7,6,8,6,7,5,6,7,5,1,2,5,7], label: 'granualy'}
  ];
  public chartLabels1: Array<any> = ['2013-10', '2014-02', '2014-03', '2014-06', '2014-07', '2014-07','2014-10','2014-12','2015-01','2015-03','2015-05','2015-06','2015-07','2015-08','2015-09','2015-10','2015-11','2015-12','2016-01','2016-02','2016-03','2016-04','2016-05','2016-08'];

  
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
