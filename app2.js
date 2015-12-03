(function () {
    var app = angular.module('nodesDataModule', ["chart.js"]);


    var nodesInfo = new Array();

    app.controller('nodesDataController', ['$http', '$scope', function ($http, $scope) {
        var outerAlias = this;
        var portN="8080";
        $scope.changePort8090 = function(){

            portN = "8090";
            console.log("set"+portN);
        }

        $scope.changePort8080 = function(){

            portN = "8080";
            console.log("set"+portN);
        }

        $scope.isSelected1 = false;
        $scope.toggleButtonState1 = function(){
            $scope.isSelected1 = !$scope.isSelected1;
        }

        $scope.isSelected2 = false;
        $scope.toggleButtonState2 = function(){
            $scope.isSelected2 = !$scope.isSelected2;
        }



        $scope.removeNodes = function(nodeNo){
            $http.delete('http://localhost:'+portN+'/nodes/'+ nodeNo).success(function(deletedata) {

                console.log('http://localhost:'+portN+'/nodes/'+ nodeNo);
                $http.get('http://localhost:'+portN+'/nodes').success(function (data) {
                    outerAlias.nodes = data.allnodes;
                    outerAlias.nodes = data.allnodes;
                    console.log(data.allnodes);
                    var labelsInfo = new Array();
                    var nokey = new Array();

                    for (var i = 0; i < data.allnodes.length; i++) {
                        labelsInfo[i] = "Node-" + data.allnodes[i].port;
                        nokey[i] = data.allnodes[i].count;
                    }

                    outerAlias.labels = labelsInfo;
                    outerAlias.data = nokey;

                });
            });
        }
        $scope.addNodes = function(port){
            $http.post('http://localhost:'+portN+'/nodes/'+port).success(function(adddata){
                console.log('http://localhost:'+portN+'/nodes/'+port);
                $http.get('http://localhost:'+portN+'/nodes').success(function (data) {
                    outerAlias.nodes = data.allnodes;
                    outerAlias.nodes = data.allnodes;
                    console.log(data.allnodes);
                    var labelsInfo = new Array();
                    var nokey = new Array();

                    for (var i = 0; i < data.allnodes.length; i++) {
                        labelsInfo[i] = "Node-" + data.allnodes[i].port;
                        nokey[i] = data.allnodes[i].count;
                    }

                    outerAlias.labels = labelsInfo;
                    outerAlias.data = nokey;

                });
            });
        }

        $scope.addKV = function(key,val){
            $http.put('http://localhost:'+portN+'/keys/'+key+'/'+val).success(function(putdata){
                console.log('http://localhost:'+portN+'/keys/'+key+'/'+val);

                alert("Key Value pair is stored");

                $http.get('http://localhost:'+portN+'/nodes').success(function (data) {
                    outerAlias.nodes = data.allnodes;
                    outerAlias.nodes = data.allnodes;
                    console.log(data.allnodes);
                    var labelsInfo = new Array();
                    var nokey = new Array();

                    for (var i = 0; i < data.allnodes.length; i++) {
                        labelsInfo[i] = "Node-" + data.allnodes[i].port;
                        nokey[i] = data.allnodes[i].count;
                    }

                    outerAlias.labels = labelsInfo;
                    outerAlias.data = nokey;



                });

            });
        }

        $scope.getV = function(key) {
            $http.get('http://localhost:'+portN+'/keys/'+key).success(function(getdata){
                console.log('http://localhost:'+portN+'/keys/'+key);
                console.log(getdata);
                console.log(getdata.key);
                $scope.getval = getdata.value;




            });
        }

        $http.get('http://localhost:'+portN+'/nodes').success(function (data) {
            console.log('http://localhost:'+portN+'/nodes');
            outerAlias.nodes = data.allnodes;

            console.log(data.allnodes);
            var labelsInfo = new Array();
            var nokey = new Array();

            for (var i = 0; i < data.allnodes.length; i++) {
                labelsInfo[i] = "Node-" + data.allnodes[i].port;
                nokey[i] = data.allnodes[i].count;
            }

            outerAlias.labels = labelsInfo;
            outerAlias.data = nokey;

        });




    }]);


})();