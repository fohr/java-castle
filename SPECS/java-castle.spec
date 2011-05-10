Name:           java-castle
Version:        %{buildver}
Release:        %{buildrev}
Summary:        Acunu Castle package

Group:          Filesystem
License:        No
Source0:        %{name}-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Provides:       %{name}-%{changesetver}

BuildRequires:  ant
BuildRequires:  ant-nodeps
BuildRequires:  java-devel >= 1:1.6.0
BuildRequires:  jpackage-utils
BuildRequires:  libcastle-devel

Requires:       java >= 1:1.6.0
Requires:       jpackage-utils

%description
package com.acunu.castle for userspace access to castle-fs

%prep
%setup -q -n %{name}
ant clean

%build
ant

%install
rm -rf %{buildroot}
ant -Dbuildroot=%{buildroot} -Ddest.libs=%{buildroot}%{_libdir}/%{name} -Ddest.docs=%{buildroot}%{_docdir}/%{name} install
chmod +x %{buildroot}%{_libdir}/%{name}/libCastleImpl.so

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{_libdir}/%{name}/castle.jar
%{_libdir}/%{name}/libCastleImpl.so
%{_docdir}/%{name}

%changelog
