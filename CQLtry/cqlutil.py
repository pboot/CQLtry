# -*- coding: utf-8 -*-
"""
Created on Fri Dec 21 16:11:57 2018

@author: PB
"""

class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class InputError(Error):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """
