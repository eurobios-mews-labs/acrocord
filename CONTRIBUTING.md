# How to contribute
Thank you for your interest in the package, if you have any suggestion or encountering problem please inform us [here](https://github.com/eurobios-scb/acrocord/issues). You can also start conversation here to submit new ideas or thoughts


### Contribution process

Here is the procedure for contribution
1. Create a **fork** of the project [here](https://github.com/eurobios-scb/acrocord/fork)
2. Clone the repository and install requirements and development requirements with
```shell 
pip install -r requirements.txt
pip install -r requirements_dev.txt
```
3. Create one branch per feature you want to suggest and push them on your fork
4. Test your work by running 
```shell
pytest .
```
6. Once your feature is finished you have to sync the branch of your fork with the main branch and rebase your work
7. Create a pull request and make sure the Github actions run successfuly

For small contributions (like typos) you can directly edit in using the Github web editor.

### Contribution rule

* Please follow the [Python Naming Conventions](https://pep8.org/#prescriptive-naming-conventions) and other [PEP8](https://pep8.org/) guidelines. Try to avoid trailing whitespaces whenever possible.
* Fill the docstring of your functions in the [numpydoc](https://numpydoc.readthedocs.io/en/latest/) fashion
* Test every function and name it in the most explanatory way `test_name_of_function_behaviour_tested` in the associated file 

### Hide your email adress in commits
Check the box `Keep my email adresses private` [here](https://github.com/settings/emails) and run 
```shell 
git config --global user.email "<ID+username>@users.noreply.github.com"
```
