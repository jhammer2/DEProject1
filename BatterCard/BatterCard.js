import fetch from 'node-fetch';
import fs from 'fs';
import csv from 'fast-csv';

const endpoint = 'people';
const endpointDraft = 'draft/prospects'

const playerId = '458731';
const params = 'hydrate=awards,education,stats(group=hitting,type=yearByYear)';

const apiurl = 'https://statsapi.mlb.com/api/v1/'+endpoint+'/'+playerId+'?'+params;
const url = apiurl;

//set up the csv streams for writing data to files
//Person
const ps = fs.createWriteStream('./Output/person.csv');
const stream = csv.format();
stream.pipe(ps);

//Person
stream.write([ 'id', 'lastName', 'firstName', 'height', 'weight', 'birthDate', 'birthCity', 'birthStateProvince', 'birthCountry', 'headshotUrl', 'draftTeam', 'draftYear', 'draftRound', 'draftPick', 'bats', 'throws', 'schoolName', 'position','mlbDebutDate' ]);

//Stat
const ss = fs.createWriteStream('./Output/stats.csv');
const statStream = csv.format();
statStream.pipe(ss);

//PlayerStat
statStream.write([ 'id','season','statGamesPlayed','statGroundOuts','statAirOuts','statRuns','statDoubles','statTriples','statHomeRuns','statStrikeOuts','statBaseOnBalls','statIntentionalWalks','statHits','statHitByPitch','statAvg','statAtBats','stat.obp','statSlg','statOps','statCaughtStealing','statStolenBases','statStolenBasePercentage','statGroundIntoDoublePlay','statNumberOfPitches','statPlateAppearances','statTotalBases','statRbi','statLeftOnBase','stat.sacBunts','statSacFlies','statBabip','statGroundOutsToAirouts','statAtBatsPerHomeRun','teamId','teamName','teamLink','leagueId','leagueName','leagueLink','sportId','sportLink','sportAbbreviation','gameType' ]);

//Teams
const ts = fs.createWriteStream('./Output/teams.csv');
const teamStream = csv.format();
teamStream.pipe(ts);

//PlayerTeam(s)
teamStream.write([ 'teamid','team_name','logo_url','current' ]);

//Awards
const as = fs.createWriteStream('./Output/awards.csv');
const awardStream = csv.format();
awardStream.pipe(as);

//PlayerAwards
awardStream.write([ 'mlbamid','name','season' ]);

//fetch the Player data  from api
Promise.all([
fetch(url)
  .then(res => res.json()),
])
 .then(json => setDraftData(JSON.stringify(json)) ) 
  .catch(err => console.error(err));




function setDraftData(data) {

 const dd = JSON.parse(data);  
 const draftapiurl = 'https://statsapi.mlb.com/api/v1/'+endpointDraft+'/'+dd[0].people[0].draftYear+'?playerId='+playerId

 //make another api call to get draft data as well 
 Promise.all([
  fetch(url)
    .then(res => res.json()),
  fetch(draftapiurl)
    .then(res => res.json()),
  ])
 	.then(json => setCardData(JSON.stringify(json)) ) 
    .catch(err => console.error(err));
}


function setCardData(data) {
  
 const personInfo = [];
 const ppl=JSON.parse(data);
  
 personInfo.push(ppl[0].people[0]);
 const play = {};
 //temporarily save chunks of data as 'output' that will be written to file//
 let output = [];

   play.id = personInfo[0].id;
   play.lastName = personInfo[0].lastName;
   play.firstName = personInfo[0].firstName;   
   play.height = personInfo[0].height;
   play.weight = personInfo[0].weight;
   play.birthDate = personInfo[0].birthDate;
   play.birthCity = personInfo[0].birthCity;
   play.birthStateProvince = personInfo[0].birthStateProvince;
   play.birthCountry = personInfo[0].birthCountry;
   play.headshotUrl = ppl[1].prospects[0].headshotLink;
   play.draftTeam = ppl[1].prospects[0].team.name;
   play.draftYear = ppl[1].prospects[0].year;
   play.draftRound = ppl[1].prospects[0].pickRound;
   play.draftPick = ppl[1].prospects[0].pickNumber;
   play.bats = personInfo[0].batSide.description;
   play.throws = personInfo[0].pitchHand.description;
   play.schoolName = personInfo[0].education.colleges[0].name; //getSchoolName
   play.position = personInfo[0].primaryPosition.abbreviation; //position   
   play.mlbDebutDate = personInfo[0].mlbDebutDate
     
	//push data to the output array//
    output.push( play );

	//send output to be written to file//
  	writeToFile(output,'person');
  	
 // Build Player Stat Info
 const playerStat = {};

 //temporarily save chunks of data as 'output' that will be written to file//
 let statOutput = [];
  
//Loop through each year for the player and pull out stats
  for (let s = 0; s < JSON.stringify(personInfo[0].stats[0].splits.length); s++) {    		
  		//set the stat fields
  		 playerStat.id = personInfo[0].id;
  		 playerStat.season = personInfo[0].stats[0].splits[s].season;
  		 playerStat.statGamesPlayed = personInfo[0].stats[0].splits[s].stat.gamesPlayed;
  		 playerStat.statGroundOuts = personInfo[0].stats[0].splits[s].stat.groundOuts;
  		 playerStat.statAirOuts = personInfo[0].stats[0].splits[s].stat.airOuts;
  		 playerStat.Runs = personInfo[0].stats[0].splits[s].stat.runs;
  		 playerStat.statDoubles = personInfo[0].stats[0].splits[s].stat.doubles;
  		 playerStat.statTriples = personInfo[0].stats[0].splits[s].stat.triples;
  		 playerStat.statHomeruns = personInfo[0].stats[0].splits[s].stat.homeRuns;
  		 playerStat.statStrikeOuts = personInfo[0].stats[0].splits[s].stat.strikeOuts;
  		 playerStat.statBaseOnBalls = personInfo[0].stats[0].splits[s].stat.baseOnBalls;
  		 playerStat.statIntentionalWalks = personInfo[0].stats[0].splits[s].stat.intentionalWalks;
  		 playerStat.statHits = personInfo[0].stats[0].splits[s].stat.hits;
  		 playerStat.statHitByPitch = personInfo[0].stats[0].splits[s].stat.hitByPitch;
  		 playerStat.statAvg = personInfo[0].stats[0].splits[s].stat.avg;
  		 playerStat.statAtBats = personInfo[0].stats[0].splits[s].stat.atBats;
  		 playerStat.statObp = personInfo[0].stats[0].splits[s].stat.obp;
  		 playerStat.statSlg = personInfo[0].stats[0].splits[s].stat.slg;
  		 playerStat.statOps = personInfo[0].stats[0].splits[s].stat.ops;
  		 playerStat.statCaughtStealing = personInfo[0].stats[0].splits[s].stat.caughtStealing;
  		 playerStat.statStolenBases = personInfo[0].stats[0].splits[s].stat.stolenBases;
  		 playerStat.statStolenBasePercentage = personInfo[0].stats[0].splits[s].stat.stolenBasePercentage;
  		 playerStat.statGroundIntoDoublePlay = personInfo[0].stats[0].splits[s].stat.groundIntoDoublePlay;
  		 playerStat.statNumberOfPitches = personInfo[0].stats[0].splits[s].stat.numberOfPitches;
  		 playerStat.statPlateAppearances = personInfo[0].stats[0].splits[s].stat.plateAppearances;
  		 playerStat.statTotalBases = personInfo[0].stats[0].splits[s].stat.totalBases;
  		 playerStat.statRbi = personInfo[0].stats[0].splits[s].stat.rbi;
  		 playerStat.statLeftOnBase = personInfo[0].stats[0].splits[s].stat.leftOnBase;
  		 playerStat.statSacBunts = personInfo[0].stats[0].splits[s].stat.sacBunts;
  		 playerStat.statSacFlies = personInfo[0].stats[0].splits[s].stat.sacFlies;
  		 playerStat.statBabip = personInfo[0].stats[0].splits[s].stat.babip;
  		 playerStat.statGroundOutsToAirOuts = personInfo[0].stats[0].splits[s].stat.groundOutsToAirouts;
  		 playerStat.statAtBatsPerHomeRun = personInfo[0].stats[0].splits[s].stat.atBatsPerHomeRun;
  		 //Team info  		 
  		 playerStat.teamId = personInfo[0].stats[0].splits[s].team.id;
  		 playerStat.teamName = personInfo[0].stats[0].splits[s].team.name;
  		 playerStat.teamLink = personInfo[0].stats[0].splits[s].team.link;
  		 //League Info
  		 playerStat.leagueId = personInfo[0].stats[0].splits[s].league.id;
  		 playerStat.leagueName = personInfo[0].stats[0].splits[s].league.name;
  		 playerStat.leagueLink = personInfo[0].stats[0].splits[s].league.link;
  		 //Sport Info
  		 playerStat.sportId = personInfo[0].stats[0].splits[s].sport.id;
  		 playerStat.sportName = personInfo[0].stats[0].splits[s].sport.link;
  		 playerStat.sportName = personInfo[0].stats[0].splits[s].sport.abbreviation;
  		 //Game Info
  		 playerStat.gameType = personInfo[0].stats[0].splits[s].gameType;
  		
		//push data to the output array//
    	statOutput.push( playerStat );

		//send output to be written to file//
  		writeToFile(statOutput,'stat');  
  }
  
  //Build Player Team Info
  const team = {};
  
  //temporarily save chunks of data as 'output' that will be written to file//
  let teamOutput = [];
  
  //Loop through each year for the player and pull out stats
  for (let t = 0; t < personInfo[0].stats[0].splits.length; t++) {
   		
   	team.teamid = personInfo[0].stats[0].splits[t].team.id;
   	team.team_name = personInfo[0].stats[0].splits[t].team.name;
   	team.logo_url = 'https://www.mlbstatic.com/team-logos/'+personInfo[0].stats[0].splits[t].team.id+'.svg';
      
   	if (t == personInfo[0].stats[0].splits.length-1)
   	{
     	team.current = 'TRUE';
   	}
   	else
   	{ 
     	team.current = 'FALSE';
   	}
    
   	//push data to the output array//
   	teamOutput.push( team );

  }
  
  // Get Distinct team rows
  let result = []
  
  const map = new Map();
  for (const item of teamOutput)
  {
	  if(!map.has(item.id))
	  {
		  map.set(item.id, true);
		  result.push({
			  teamid: item.teamid,
			  team_name: item.team_name,
			  logo_url: item.logo_url,
			  current: item.current
			  
		  });
	  }
  }
  
  //send output to be written to file//
  writeToFile(result,'teams');

  // Player Awards Info

  const award = {};
  
  //temporarily save chunks of data as 'output' that will be written to file//
  let awardOutput = [];

  //Loop through each year for the player and pull out stats
  for (let a = 0; a < personInfo[0].awards.length; a++) {
	if (personInfo[0].awards[a].id == 'ALGG' )
	{
		award.mlbamid = personInfo[0].awards[a].player.id
   		award.name = personInfo[0].awards[a].name;
   		award.season = personInfo[0].awards[a].season;
  	
  		awardOutput.push( award );	
  		
  		// Get Distinct team rows
  		let result1 = []
  
  		const map = new Map();
  		for (const item of awardOutput)
  		{
	  		if(!map.has(item.mlbamid))
	  		{
		  		map.set(item.mlbamid, true);
		  		result1.push({
			  					mlbamid: item.mlbamid,
			  					name: item.name,
			  					season: item.season,
		  		});
	  		}
  		}
  
		  //send output to be written to file//
		  writeToFile(result1,'awards');
	}
  }
}

 //takes an array and writes each line to csv file//
 function writeToFile(data,fileType) {
   if (fileType == 'person')
   {
  	 data.forEach( (row) => stream.write(row) );
   }
   else if (fileType == 'stat')
   {
  	 data.forEach( (row) => statStream.write(row) );
   }
   else if (fileType == 'teams')
   {
  	 data.forEach( (row) => teamStream.write(row) );
   }
   else if (fileType == 'awards')
   {
  	 data.forEach( (row) => awardStream.write(row) );
   }
 }