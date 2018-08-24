'use strict';

var Pivot = Stapes.subclass({
    constructor: function(options){
        this.options = (options || {});

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

    formatQueryField: function() {
        var value = $('input[name="q"]').val();
        var criteria = $('.filter-criteria');

        criteria.empty();

        if (value) {
            var parts = value.split('/');
            var fixed = [];

            // console.debug('parts', parts)

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

                    var criterion = $('<span></span>');
                    criterion.addClass('criterion');

                    criterion.append(
                        $('<span></span>').addClass('criterion-field').text(field)
                    );

                    criterion.append(
                        $('<span></span>').addClass('criterion-operator').text(op)
                    );

                    criterion.append(
                        $('<span></span>').addClass('criterion-value').text(value)
                    );

                    criteria.append(criterion);
                    fixed.push(field + '/' + op + ':' + value);
                }
            }

            $('input[name="q"]').val(fixed.join('/').trim());

            // console.debug('fixed', $('input[name="q"]').val());
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

        var q = $('input[name="q"]').val();
        var rx = new RegExp(field + '/' + op + ':' + value + '/?');

        q = q.replace(rx, '');
        q = q.trim();

        $('input[name="q"]').val(q);

        this.formatQueryField();

        if (remove) {
            this.updateQuery();
        }
    },

    appendCriterion: function() {
        var q = $('input[name="q"]').val().trim();
        var field = $('input[name="field"]').val();
        var operator = $('select[name="operator"]').val();
        var value = $('input[name="value"]').val();

        if (field.length && operator.length && value.length) {
            if (q.length) {
                q += '/';
            }

            q += field;
            q += '/';
            q += operator;
            q += ':';
            q += value;

            $('input[name="q"]').val(q);
            this.formatQueryField();
            this.updateQuery();
        }
    },

    updateQuery: function() {
        location.href = location.href.replace(/([\?\&])q=[^\&]*/, '$1q=' + $('input[name="q"]').val());
    },
});

$(document).ready(function(){
    window.pivot = new Pivot({
        viewTarget: '#content',
    });
});
