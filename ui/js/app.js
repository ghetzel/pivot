'use strict';

var Pivot = Stapes.subclass({
    constructor: function(options){
        this.options = (options || {});
    },
});

$(document).ready(function(){
    window.pivot = new Pivot({
        viewTarget: '#content',
    });
});
