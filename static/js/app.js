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
    constructor: function(){
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

        this.backends = {};
        this._oldpath = null;

        this.site = {
            title: 'Pivot Manager',
            brand: 'Pivot',
        };
    },

    run: function(){
        this._boundFullPage = rivets.bind($('html'), this);

        this._router = Router({
            '/backends/:id': function(id){
                this.chpage('backend',
                    new BackendController(this.backends[id], this));
            }.bind(this),
        });

        this.load(function(){
            console.debug('Load complete');
            this._router.init();
            $('body').removeAttr('style');
            $('title').text(this.site.title);
        }.bind(this));
    },

    load: function(callback){
        this._loadCallback = callback;
        this._loadOutstanding = 1;

        $.ajax({
            url: '/api/backends',
        }).success(function(data){
            pivot.backends = data.payload;
            pivot._checkLoad();
        });
    },

    chpage: function(template, controller) {
        console.debug('chpage', template, controller);

        $('#content').load('/views/'+template+'.html', null, function(content, status){
            console.debug('Loaded', template, arguments);

            switch(status){
            case 'success':
                if(controller){
                    if(this._boundContent){
                        rivets.unbind(this._boundContent);
                    }

                    rivets.bind($('#content'), controller);
                }

                break;

            default:
                $('#content').load('/views/error.html', null, function(content, status){
                    if(status == 'error'){
                        $('#content').text('Failed to load template "'+template+'". Additionally, an error page is not configured.');
                    }
                });
            }
        }.bind(this));
    },

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
    window.pivot = new Pivot();
    pivot.run();
});
