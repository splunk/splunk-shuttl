#!/bin/bash

/sbin/ifconfig -a | perl -ne 'if ( m/^\s*inet (?:addr:)?([\d.]+).*?cast/ ) { print qq($1\n); exit 0; }'
