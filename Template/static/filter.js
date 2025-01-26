const operation_mapping = {
	'Contains':'like',
	'Not contains':"=",
	'Equals': "=",
	'Not equal': "!=",
	'Starts with':"starts",
	'Ends with':"ends", 
	'Blank':"=",
	"Not blank": "!=", 
	"Greater than": ">",
	"Greater than or equals": ">=",
	"Less than": "<",
	"Less than or equals": "<=",
	"In range":"in",
	"Increased by %": "=",
	"Decreased by %": "="
} 


const field_mapping = {
	// String Filter
	'Campaign':'campaign_name',
	'Keyword':'keyword_name',
	'Target':'Placement_Type',
	'Ad Group':'ad_group_name',
	'Product':'product_name',
	'Portfolio':'Portfolio',
	'Campaign Tags':'campaign_tags',
	'Market Place':'market_place',
	'Search Term': 'search_term',
	'Campaign Type':'type', //campaign type
	'Target Type':'ttype', //keyword target type
	'Impressions': 'impressions',
	'Clicks':'clicks',
	'Spends':'spends',
	'Sales': 'sales',
	'CTR': 'ctr',
	'TROAS': 'troas',
	'ROAS': 'roas',
	'ACOS': 'acos',
	'Direct ATC': 'direct_atc',
	'Total Ad Sales': 'total_ad_sales',
	'Campaign ID':'campaign_id',
	'Direct ATC':'direct_atc',
	'Orders':'orders',
	'CPM':'cpm',
	'Overall Rank':'overall_rank',
	'Keyword Class':'keyword_class',
	'Program Type':'program_type',
	'Placement Type':'placement_type',
	'ROI':'roi',
	'CVR':'cvr',
	
	// 'Target Type':'target_type',
	// Numeric Filters
	
}




const filt_containers = document.querySelectorAll(".accordion-collapse");


filt_containers.forEach(function(container){
	container.addEventListener('mouseenter', function(){
		container.querySelector('select').addEventListener('change', ()=>{
		if(container.querySelector('select').value === 'Blank' || container.querySelector('select').value === "Not blank"){
			container.querySelector('#jinx').value = ""
			container.querySelector('#jinx').classList.add('disable')
			}
		else{
			container.querySelector('#jinx').classList.remove('disable')
		}
		});
	});
});






function stringfilter(id){
 // const field_mapping = ['product_name','adgroup_name', 'campaign_name', 'type' ]
 const container  = document.querySelector('#collapseOne'+ id);
 var filter_type  = container.querySelector('select').value;
 var filter_input = container.querySelector('#jinx').value;
 var filter_field = container.querySelector;
 var field = container.previousElementSibling.children[0].innerText;
 // console.log(filter_type, filter_input, id);
 console.log(field_mapping[field], operation_mapping[filter_type], filter_input)
 table.setFilter(field_mapping[field], operation_mapping[filter_type], filter_input);
}


function metricfilter(id){
	const container  = document.querySelector('#collapseTwo'+ id);
	var filter_type  = container.querySelector('select').value;
    var filter_input = container.querySelector('#jinx').value;
    var field = container.previousElementSibling.children[0].innerText;
	console.log("clickity");
 	console.log(field_mapping[field], operation_mapping[filter_type], filter_input)
 	table.setFilter(field_mapping[field], operation_mapping[filter_type], filter_input);
}

//-------------------- Column Reference-----------------//
// {campaign_id: "LDYTKNV2LYWZ", 
// campaign_name: "HM_PLA_BL_Whites_Focus_Brand_March23",
// ad_group_id: "KSS48G842365",
// adgroup_name: "Body and Face Skin Care",
// adspend: "59.4",
// adspend_change: 200,
// campaign_id: "LDYTKNV2LYWZ",
// campaign_name: "HM_PLA_BL_Whites_Focus_Brand_March23",
// clicks: "596.0",
// clicks_change: 446.79,
// ctr: "37.5766",
// ctr_change: 459.38,
// cvr: "170.0",
// cvr_change: 261.7,
// direct_revenue: "29216.0",
// direct_revenue_change: 747.82,
// fsn_id: "MSCF3PSZFVDFRBVX",
// indirect_revenue: "23526.0",
// indirect_revenue_change: 548.46,
// product_name: "NIVEA Body Lotion, Aloe Hydration, with Aloe Vera, for Men & Women",
// roas: "20.5764",
// roas_change: 157.28,
// roi_direct: "36.0",
// roi_direct_change: 414.29,
// roi_indirect: "20.0",
// roi_indirect_change: 150,
// troas: "0.6582",
// troas_change: 39.1,
// type: "PLA",
// units_sold_direct: "88.0",
// units_sold_direct_change: 780,
// units_sold_indirect: "80.0",
// units_sold_indirect_change: 515.38,
// views: "13536.0",
// views_change: 298.94 }



const expo =  document.querySelector("#export");


expo.addEventListener("click", ()=>{
	table.download("xlsx", "reports.xlsx", {sheetName:"Data"});
	 
	table.download("pdf", "PDFReports.pdf", {
        orientation:"landscape", //set page orientation to portrait
        title:"Dashboard Reports", //add title to report
    });
});