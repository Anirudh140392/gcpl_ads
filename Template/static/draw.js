
const tclick = document.querySelector("#click_group");
const tad_sales = document.querySelector('#sale_group');
const tview = document.querySelector('#impression_group');
const torders = document.querySelector('#order_group');
const tad_spend = document.querySelector('#spend_group');
const links = document.querySelectorAll(".nav-link");
const ctr = document.querySelector("#ctr_group");
const cvr = document.querySelector("#cvr_group");
const acos = document.querySelector("#acos_group");
const cpc = document.querySelector("#cpc_group");
const aov = document.querySelector("#aov_group");






function color_box(list){
var colors = []
var col = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F']
for(var i=0; i <= list.length; i++){
  color = "#" + col[Math.round((Math.random() *10))] 
          + col[Math.round((Math.random() *10))] 
          + col[Math.round((Math.random() *10))] 
          + col[Math.round((Math.random() *10))] 
          + col[Math.round((Math.random() *10))]
          + col[Math.round((Math.random() *10))];
  colors.push(color);  
}
return colors;
}

links.forEach(function(link){
  link.addEventListener('click', function(){
    link.classList.add("fw-bolder")
  })
})



console.log(datamapping);

// Charts.js


const range = []

const labels = datamapping['date'];


const data = {
  labels: labels,
  datasets: [
    {
      label: 'VIEWS',
      data: datamapping['impressions'],
      backgroundColor:'rgba(0, 194, 239, 1)',
      borderColor: 'rgba(0, 194, 239, 1)',
      fill:false,
    },
    {
      label:"CLICKS",
      backgroundColor:'rgba(255, 158, 37, 1)',
      borderColor: 'rgba(255, 158, 37, 1)' ,
      data: datamapping['clicks'],
      fill:false, 
},

  ]
};




const config = {
  type: 'line',

  data: data,

  options: {
    responsive: true,
    maintainAspectRatio: true,
    legend: {
      display: true,
      labels: {
          usePointStyle: true,
              },
    
    },
}


};



const chart =  new Chart("bar", config)




function handler(chart, val=0) {
  const data = chart.data;
  const newDataset = [{
      label:"CLICKS",
      backgroundColor:'rgba(0, 255, 128, 1)',
      borderColor: 'rgba(0, 255,128, 1)' ,
      data: datamapping['clicks'],
      fill:false, 
},
{
      label:"IMPRESSIONS",
      backgroundColor:'rgba(208,240,192, 1)',
      borderColor: 'rgba(42, 84, 84, 1)' ,
      data: datamapping['impressions'] ,
      fill:false,
},
{
      label:"AD SALES",
      backgroundColor:'rgba(240, 96, 99, 1)',
      borderColor: 'rgba(240, 96, 99, 1)' ,
      data: datamapping['cdcr'] ,
      fill:false,
},

{
      label:"ORDERS",
      backgroundColor:'rgba(255, 128, 255, 1)',
      borderColor: 'rgba(255, 128, 255, 1)' ,
      data: datamapping['cdcu'] ,
      fill:false,
},
{
      label:"AD SPENDS",
      backgroundColor:'rgba(255, 128, 0, 1)',
      borderColor: 'rgba(255, 128, 0, 1)' ,
      data: datamapping['ads_pend'] ,
      fill:false,
},
{
      label:"CTR",
      backgroundColor:'rgba(128, 255, 255, 1)',
      borderColor: 'rgba(128, 255, 255, 1)' ,
      data: datamapping['ctr'] ,
      fill:false,
},
{
      label:"CVR",
      backgroundColor:'rgba(255, 128, 128, 1)',
      borderColor: 'rgba(255, 128, 128, 1)' ,
      data: datamapping['cvr'] ,
      fill:false,
},
{
      label:"ACOS",
      backgroundColor:'rgba(112, 146, 190, 1)',
      borderColor: 'rgba(112, 146, 190, 1)' ,
      data: datamapping['roi'] ,
      fill:false,
},
{
      label:"CPC",
      backgroundColor:'rgba(255, 128, 128, 1)',
      borderColor: 'rgba(255, 128, 128, 1)' ,
      data: datamapping['cicr'],
      fill:false, 
},
{
      label:"aov",
      backgroundColor:'rgba(128, 255, 255, 1)',
      borderColor: 'rgba(128, 255, 255, 1)' ,
      data: datamapping['cicu'],
      fill:false, 
},

]
if(chart.data.datasets.length==2){
  chart.data.datasets.shift();
}
  chart.data.datasets.push(newDataset[val]);
  chart.update();
}  














tclick.addEventListener('click', ()=>{
  console.log("I was clicked");
  handler(chart, 0);  
})

tad_spend.addEventListener('click', ()=>{
  handler(chart, 4)
})

tad_sales.addEventListener('click', ()=>{
  handler(chart, 2)
})

tview.addEventListener('click', ()=>{
  handler(chart, 1)
})

torders.addEventListener('click', ()=>{
  handler(chart, 3)
  })


ctr.addEventListener('click', ()=>{
  handler(chart, 5)
  })


cvr.addEventListener('click', ()=>{
  handler(chart, 6)
  })


acos.addEventListener('click', ()=>{
  handler(chart, 7)
  })


// cpc.addEventListener('click', ()=>{
//   handler(chart, 8)
//   })


// aov.addEventListener('click', ()=>{
//   handler(chart, 9)
//   })






