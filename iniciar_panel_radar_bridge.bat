@echo off
title Panel Radar Bridge
cd /d "%~dp0"
py panel_radar_bridge.py
if errorlevel 1 (
  echo.
  echo No se pudo iniciar el puente.
  echo Verifique que Python este instalado.
  pause
)
