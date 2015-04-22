module.exports = function (grunt) {

    // Project configuration.
    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),
        bower: {
                install: {
                             
                         }
        },
       uglify: {
            options: {
                banner: '/*! <%= pkg.name %> <%= grunt.template.today("yyyy-mm-dd") %> */\n'
            },
            build: {
                src: 'build/<%= pkg.name %>.js',
                dest: 'build/<%= pkg.name %>.min.js'
            }
        },
        jslint: {
            client: {
                src: [
                    'src/*.js'
                ],
                directives: {
                    browser: true,
                    predef: [
                        '_',
                        'underscore',
                        'reqwest'
                    ],
                    regexp: true,
                    bitwise: true,
                    nomen: true
                },
                options: {

                }
            }
        },
        concat: {
            dist: {
                // the files to concatenate
                src: ['js/bower.js', 'src/rlNoConflict.js', 'src/rlSession.js', 'src/rlClient.js'],
                // the location of the resulting JS file
                dest: 'build/<%= pkg.name %>.js',
                options: {
                    banner: ";((function( window ){ \n 'use strict';",
                    footer: "window.RL = {}; RL.client = rlClient; RL.session = rlSession }).call(window, window ));"
                }
            }
        },

        
        bower_concat: {
            all: {
                dest: 'js/bower.js'
            }
        }
    });

    grunt.loadNpmTasks('grunt-bower-task');
    // Load the plugin that provides the "uglify" task.
    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-contrib-concat');
    // Default task(s).
    grunt.registerTask('default', ['jslint','bower','bower_concat','concat','uglify']);

    // concatenate libs
    grunt.loadNpmTasks('grunt-bower-concat');
    grunt.loadNpmTasks('grunt-jslint');

};