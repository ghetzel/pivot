'use strict';

var BackendController = Stapes.subclass({
    constructor: function(data, parent){
        if(parent){
            this.extend(parent);
            this.$parent = parent;
        }

        if(data){
            this.extend(data);
            this.$self = data;
        }
    }
});

var Pivot = Stapes.subclass({
    constructor: function(options){
        this.site = {
            title: 'Pivot Manager',
            brand: 'Pivot',
        };

        this.extend(options || {});

        rivets.configure({
             templateDelimiters: ['{{', '}}'],
        });

        this._boundContent = null;

        rivets.formatters.interpolate = function(value){
            var path = [];

            $.each(Array.prototype.slice.call(arguments, 1), function(i, arg){
                path.push(arg.replace('${value}', value));
            });

            return path.join('');
        };

        rivets.formatters.jsonify = function(value){
            return JSON.stringify(value, null, 2);
        };

        rivets.formatters.values = function(value){
            var values = [];

            switch(typeof value) {
            case 'object':
                $.each(value, function(_, v){
                    values.push(v);
                });

                break;

            case 'array':
                values = value;
                break;

            default:
                values = [value];
                break;
            }

            return values;
        };

        this.backends = {};
        this._oldpath = null;
    },

    // Setup page bindings and routes and perform the initial data load, then
    // show everything.
    //
    run: function(){
        this._boundFullPage = rivets.bind($('html'), this);

        this._router = Router({
            '/backends/:id': function(id){
                this.chpage('backend',
                    new BackendController(this.backends[id], this));
            }.bind(this),

            '/': function(){
                this.chpage('index');
            }.bind(this),
        });

        this.load(function(){
            this.chpage('index');
            this._router.init();
            $('body').removeAttr('style');
            $('title').text(this.site.title);
        }.bind(this));
    },

    // Load the API data the site depends on
    //
    load: function(callback){
        this._loadCallback = callback;

        // should equal the number of $.ajax calls that happen below,
        // and each call (when successful) should call _checkLoad()
        this._loadOutstanding = 1;

        // load details about all backends
        $.ajax({
            url: '/api/backends',
        }).success(function(data){
            pivot.backends = data.payload;
            pivot._checkLoad();
        });
    },

    // Attempt to load the content of the named template into the container pointed at by viewTarget
    //
    chpage: function(template, controller) {
        $(this.viewTarget).load('/views/'+template+'.html', null, function(content, status){
            switch(status){
            case 'success':
                if(controller){
                    if(this._boundContent){
                        rivets.unbind(this._boundContent);
                    }

                    rivets.bind($(this.viewTarget), controller);
                }

                break;

            default:
                $(this.viewTarget).load('/views/error.html', null, function(content, status){
                    if(status == 'error'){
                        $(this.viewTarget).text('Failed to load template "'+template+'". Additionally, an error page is not configured.');
                    }
                });
            }
        }.bind(this));
    },

    // If a load callback was given, decrement the outstanding calls counter
    // and fire the callback if the counter is <= 0
    //
    _checkLoad: function(){
        if(typeof this._loadCallback === 'function') {
            this._loadOutstanding--;

            if(this._loadOutstanding <= 0){
                this._loadCallback();
                this._loadCallback = undefined;
            }
        }
    }
});

$(document).ready(function(){
    window.pivot = new Pivot({
        viewTarget: '#content',
    });

    pivot.run();
});
