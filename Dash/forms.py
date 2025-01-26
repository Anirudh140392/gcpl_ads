from django import forms
from django.contrib.auth.forms import AuthenticationForm, UsernameField, UserCreationForm
from django.utils.translation import gettext, gettext_lazy as _
from django.contrib.auth.models import User



# Cutomer Login Form
class LoginForm(AuthenticationForm):
    username = UsernameField(widget= forms.TextInput(attrs= {'autofocus': True, 'class': 'form-control'}))
    password = forms.CharField(label= _("Password"), strip= False,  widget= forms.PasswordInput(attrs= {'autocomplete': 'current-password', 'class': 'form-control'}))



class ChildUsrRegForm(UserCreationForm):
    email = forms.CharField(required=True, widget= forms.EmailInput(attrs= {'class': 'form-control'}))
    password1 = forms.CharField(label= 'Password', widget= forms.PasswordInput(attrs= {'class': 'form-control'}))
    password2 = forms.CharField(label= 'Confirm Password', widget= forms.PasswordInput(attrs= {'class': 'form-control'}))

    class Meta:
        model = User
        fields =  ['username', 'email', 'password1', 'password2']
        labels = {'email': 'Email'}
        widgets = {'username': forms.TextInput(attrs= {'class': 'form-control'})}