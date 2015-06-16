var movies_json = require('./movies.js');
var olympics = require('./olympics.js');
var census = require('./census.js');
var birdstrikes_json = require("./birdstrikes.js");
var bike_sharing = require("./bike_sharing.js");
var sales = require("./sales.js");
var cars = require("./cars.js");
var housing = require("./housing.js")

test_data1 = {
	order : { 
		"item" : 0,
		"price" : 1,
		"amount" : 2
	},
	type : {
		"item" : "string",
		"price" : "number",
		"amount" : "number"
	},
	data : 
	[
		["Milk", 2, 45],
		["Eggs", 5, 20],
		["Cereal", 10, 3]
	]
};	

test_data2 = {
	order : {
		"city" : 0,
		"population" : 1
	},
	type :
	{
		"city" : "string",
		"population" : "number"
	},
	data :
	[
		["Boston", 3000],
		["NY", 10000],
		["Seattle", 5000]
	]
};

cars_subset = {
	order : {
		"mpg" : 0,
		"cylinders" : 1,
		"displacement" : 2,
		"horsepower" : 3,
		"weight" : 4,
		"acceleration" : 5,
		"model-year" : 6,
		"origin" : 7,
		"name" :8
	},
	type :
	{
		"mpg" : "measure",
		"cylinders" : "dimension",
		"displacement" : "measure",
		"horsepower" : "measure",
		"weight" : "measure",
		"acceleration" : "measure",
		"model-year" : "dimension",
		"origin" : "dimension",
		"name" : "dimension"
	},
	data :
	[
		[18.0,8,307.0,130.0,3504.,12.0,70,1,"chevrolet chevelle malibu"],
		[15.0,8,350.0,165.0,3693.,11.5,70,1,"buick skylark 320"],
		[18.0,8,318.0,150.0,3436.,11.0,70,1,"plymouth satellite"],
		[16.0,8,304.0,150.0,3433.,12.0,70,1,"amc rebel sst"],
		[17.0,8,302.0,140.0,3449.,10.5,70,1,"ford torino"],
		[15.0,8,429.0,198.0,4341.,10.0,70,1,"ford galaxie 500"],
		[14.0,8,454.0,220.0,4354., 9.0,70,1,"chevrolet impala"],
		[14.0,8,440.0,215.0,4312., 8.5,70,1,"plymouth fury iii"],
		[14.0,8,455.0,225.0,4425.,10.0,70,1,"pontiac catalina"],
		[15.0,8,390.0,190.0,3850., 8.5,70,1,"amc ambassador dpl"],
		[15.0,8,383.0,170.0,3563.,10.0,70,1,"dodge challenger se"],
		[14.0,8,340.0,160.0,3609., 8.0,70,1,"plymouth 'cuda 340"],
		[15.0,8,400.0,150.0,3761., 9.5,70,1,"chevrolet monte carlo"],
		[14.0,8,455.0,225.0,3086.,10.0,70,1,"buick estate wagon (sw)"],
		[24.0,4,113.0,95.00,2372.,15.0,70,3,"toyota corona mark ii"],
		[22.0,6,198.0,95.00,2833.,15.5,70,1,"plymouth duster"],
		[18.0,6,199.0,97.00,2774.,15.5,70,1,"amc hornet"],
		[21.0,6,200.0,85.00,2587.,16.0,70,1,"ford maverick"],
		[27.0,4,97.00,88.00,2130.,14.5,70,3,"datsun pl510"],
		[26.0,4,97.00,46.00,1835.,20.5,70,2,"volkswagen 1131 deluxe sedan"]
	]
};

movies = {
	order : {
		"Title" : 0,
		"US_Gross" : 1,
		"Worldwide_Gross" : 2,
		"US_DVD_Sales" : 3,
		"Production_Budget" : 4,
		"Release_Date" : 5,
		"MPAA_Rating" : 6,
		"Running_Time_min" : 7,
		"Distributor" :8,
		"Source" : 9,
		"Major_Genre" : 10,
		"Creative_Type" : 11,
		"Director" : 12,
		"Rotten_Tomatoes_Rating" : 13,
		"IMDB_Rating" : 14,
		"IMDB_Votes" : 15
	},
	type :
	{
		"Title" : "",
		"US_Gross" : "measure",
		"Worldwide_Gross" : "measure",
		"US_DVD_Sales" : "measure",
		"Production_Budget" : "measure",
		"Release_Date" : "",
		"MPAA_Rating" : "dimension",
		"Running_Time_min" : "measure",
		"Distributor" : "dimension",
		"Source" : "dimension",
		"Major_Genre" : "dimension",
		"Creative_Type" : "dimension",
		"Director" : "",
		"Rotten_Tomatoes_Rating" : "measure",
		"IMDB_Rating" : "measure",
		"IMDB_Votes" : "measure"
	},
	data : []
};

birdstrikes = {
	order : {
		"Airport__Name": 0,
		"Aircraft__Make_Model": 1,
		"Effect__Amount_of_damage": 2,
		"Flight_Date": 3,
		"Aircraft__Airline_Operator": 4,
		"Origin_State": 5,
		"When__Phase_of_flight": 6,
		"Wildlife__Size":7,
		"Wildlife__Species": 8,
		"When__Time_of_day":9,
		"Cost__Other":10,
		"Cost__Repair":11,
		"Cost__Total_$":12,
		"Speed_IAS_in_knots":13
	},
	type : {
		"Airport__Name": "",
		"Aircraft__Make_Model": "",
		"Effect__Amount_of_damage": "dimension",
		"Flight_Date": "",
		"Aircraft__Airline_Operator": "dimension",
		"Origin_State": "",
		"When__Phase_of_flight": "dimension",
		"Wildlife__Size":"dimension",
		"Wildlife__Species": "dimension",
		"When__Time_of_day":"dimension",
		"Cost__Other":"measure",
		"Cost__Repair":"measure",
		"Cost__Total_$":"measure",
		"Speed_IAS_in_knots":"measure"
	},
	data : []
};




function turnJSONToArray (obj, json) {
	//console.log
	for (var i = 0; i < 100; i++) {
		var dict = json[i];
		var arr = Object.keys(dict).map(function(k) { return dict[k] });
		obj.data.push(arr);
	}
	//console.log(obj.data);
	return obj;
}

module.exports = {
	datasets : {
		//"test_data1" : test_data1,
		//"test_data2" :test_data2,
		//"cars_subset" : cars_subset,
		"movies" : turnJSONToArray(movies, movies_json.movies_json),
		"olympics" : olympics.olympics,
		"birdstrikes" : turnJSONToArray(birdstrikes, birdstrikes_json.birdstrikes_json),
		"census" : census.census,
		"bike_sharing" : bike_sharing.bike_sharing,
		"sales" : sales.sales,
		"cars" : cars.cars,
		"housing" : housing.housing
	}
};

