'use strict';

var Pivot = Stapes.subclass({
    constructor: function(options){
        this.options = (options || {});
        this.query = this.splitQuery(URI(decodeURIComponent(location.href)).search(true).q || '');

        $(document).on('click', function(e) {
            if (e.button == 0) {
                var el = $(e.target);

                var criterion = el.closest('.criterion');

                if (criterion.length) {
                    this.editCriterion(criterion, false);
                } else if (el.is('.browser-view th')) {
                    $('input[name="field"]').val(el.text())
                    $('input[name="field"]').focus();
                }
            }
        }.bind(this));

        $(document).on('contextmenu', function(e) {
            var el = $(e.target);

            var criterion = el.closest('.criterion');

            if (criterion.length) {
                this.editCriterion(criterion, true);
                e.preventDefault();
            }
        }.bind(this));

        $(document).on('keypress', function(e) {
            var el = $(e.target);

            if (el.is('input[name="value"]') && e.which == 13) {
                this.appendCriterion();
            }
        }.bind(this));

        // $(document).on('input', function(e) {
        //     var el = $(e.target);

        //     if (el.is('input[name="value"]')) {
        //         var field = $('input[name="field"]').val();
        //         var value = el.val();

        //         if (field.length && value.length) {
        //             $.ajax({
        //                 url: '/api/collections/sites/list/' + field,
        //                 data: {
        //                     'q': field + '/prefix:' + value,
        //                 },
        //             }).then(function(data){
        //                 console.debug(data[field]);
        //             })
        //         }
        //     }
        // });
    },

    splitQuery: function(querystring) {
        if ($.isArray(querystring)) {
            querystring = $.grep(querystring, function(q, i) {
                return (q.length > 0);
            }).join('/');
        }

        if (querystring) {
            var parts = querystring.split('/');
            var out = [];

            for(var i = 0; i < parts.length; i+=2) {
                if (i+1 < parts.length) {
                    var field = parts[i];
                    var opValue = parts[i+1].split(':', 2);
                    var op = 'is';
                    var value = '';

                    if (opValue.length == 1) {
                        value = opValue[0];
                    } else {
                        op = opValue[0];
                        value = opValue[1];
                    }

                    out.push({
                        'field': field,
                        'operator': op,
                        'value': value,
                    });
                }
            }

            return out;
        } else {
            return [];
        }
    },

    joinQuery: function(queryset) {
        var out = [];

        $.each(queryset, function(i, q) {
            out.push(q.field + '/' + (q.operator || 'is') + ':' + q.value.toString());
        });

        return out.join('/');
    },

    formatQueryField: function() {
        var value = this.query;
        var criteria = $('.filter-criteria');

        criteria.empty();

        if (value) {
            console.debug('formatting query', this.query)

            $.each(this.query, function(i, q) {
                var criterion = $('<span></span>');
                criterion.addClass('criterion');

                criterion.append(
                    $('<span></span>').addClass('criterion-field').text(q.field)
                );

                criterion.append(
                    $('<span></span>').addClass('criterion-operator').text(q.operator)
                );

                criterion.append(
                    $('<span></span>').addClass('criterion-value').text(q.value)
                );

                criteria.append(criterion);
            }.bind(this));
        }
    },

    editCriterion: function(el, remove) {
        var field = el.find('.criterion-field').text();
        var op = el.find('.criterion-operator').text();
        var value = el.find('.criterion-value').text();

        if (!op.length) {
            op = 'is';
        }

        if (!remove) {
            $('input[name="field"]').val(field);
            $('select[name="operator"]').val(op);
            $('input[name="value"]').val(value);
        }

        this.removeCriterion(field, op, value);
        this.formatQueryField();

        if (remove) {
            this.updateQuery();
        }
    },

    removeCriterion: function(field, op, value) {
        this.query = $.grep(this.query, function(q) {
            if(q.field == field && q.operator == op && q.value == value) {
                return false;
            } else {
                return true;
            }
        });
    },

    appendCriterion: function() {
        var field = $('input[name="field"]').val();
        var operator = $('select[name="operator"]').val();
        var value = $('input[name="value"]').val();

        if (field.length && operator.length && value.length) {
            this.query.push({
                'field':    field,
                'operator': operator,
                'value':    value,
            });

            this.updateQuery();
        }
    },

    updateQuery: function() {
        var joined = this.joinQuery(this.query);

        if (joined.length) {
            window.location.href = URI(window.location.href).setSearch('q', joined).normalizeSearch();
        } else {
            window.location.href = URI(window.location.href).removeSearch('q').normalizeSearch();
        }
    },
});

$(document).ready(function(){
    window.pivot = new Pivot({
        viewTarget: '#content',
    });
});
