FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY CdcOutbox.Core/CdcOutbox.Core.csproj CdcOutbox.Core/
COPY CdcOutbox.Host/CdcOutbox.Host.csproj CdcOutbox.Host/
RUN dotnet restore CdcOutbox.Host/CdcOutbox.Host.csproj

COPY CdcOutbox.Core/ CdcOutbox.Core/
COPY CdcOutbox.Host/ CdcOutbox.Host/
RUN dotnet publish CdcOutbox.Host/CdcOutbox.Host.csproj -c Release -o /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
USER app
ENTRYPOINT ["dotnet", "CdcOutbox.Host.dll"]
