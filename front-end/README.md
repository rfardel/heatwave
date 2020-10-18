# Dash front-end

## Set up
### Requirements
Front-end requires Nginx and Gunicorn, as well as the content of `requirements.txt`.

### Database credentials
Save the username and password of a user with SELECT access to the database in `creds.json` in this folder,
following this structure:
`{`
`   "username": <USERNAME>,`
`   "password": <PASSWORD>`
`}`

## Attribution
Front-end created from the Dash sample app template
[Dash-opioid-epidemic](https://github.com/plotly/dash-sample-apps/tree/master/apps/dash-opioid-epidemic)