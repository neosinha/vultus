
function getmatches() {
 var urlx = server + '/getmatches';
	console.log('Getting matchlist: '+ urlx);
    ajaxLoad(matchlistcallback, urlx);
}


function matchlistcallback(response) {
    //console.log('Matchlist: '+ response);
    matches = JSON.parse(response);
    var colCount = 1;
    var matchcol = document.getElementById('matches');
    for (i=0; i < matches.length; i++) {
        var match = matches[i];
        console.log(JSON.stringify(match));
        }
    vueInit();
}