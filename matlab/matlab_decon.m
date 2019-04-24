function RLdecon(input_tiff, output_filepath, psf, flatfield_dir, background, dz_data, dz_psf, nIter)
warning('off','MATLAB:imagesci:tiffmexutils:libtiffWarning');

%n_cores = 4;
%maxNumCompThreads();
%disp(['default number of threads: ', num2str(threads)]);

tic
if ischar(dz_psf)
    dz_psf=str2double(dz_psf);
end
if ischar(dz_data)
    dz_data=str2double(dz_data);
end

bSaveUint16 = true
nTapering = 10; % 10px by default? It was set to 0

bReverse = false;
dz_data_ratio = 1;

if ischar(psf)
    [a,b,suffix]=fileparts(psf);
    disp(['Loading psf:     ', psf]);
    if strcmp(suffix, '.mat')
        load(psf, 'psf');
    elseif strcmp(suffix, '.tif')
        psf=psf_gen(psf, dz_psf, dz_data*dz_data_ratio, 48);
    else
        psf = [];  % dummy PSF
    end
end

psf = psf/max(psf(:))/500;

if ischar(background)
    background=str2double(background);
end
if ischar(nIter)
    nIter=str2num(nIter);
end

disp(['Loading rawdata: ', input_tiff]);
rawdata = loadtiff(input_tiff);
[x_rawdata, y_rawdata, z_rawdata] = size(rawdata);
[datafolder, inputfile, suffix] = fileparts(input_tiff);

if ~isempty(flatfield_dir)
    disp('applying flatfields');
    S = loadtiff(fullfile(flatfield_dir, 'S.tif'));
    T = loadtiff(fullfile(flatfield_dir, 'T.tif'));

    if size(size(S),2)==size(size(rawdata),2)
        rawdata = single(rawdata).*S + T;
    else %2D case
        for i =1:size(rawdata,3)
            rawdata(:,:,i) = single(rawdata(:,:,i)).*S + T;
        end
    end
end



%% Padding raw data

rawdataPadded = zeros(size(rawdata,1)+60,size(rawdata,2)+60,size(rawdata,3)+60,'single');
rawdataPadded(31:(31+size(rawdata,1)-1),31:(31+size(rawdata,2)-1),31:(31+size(rawdata,3)-1)) = rawdata;

rawdataPadded(31:(31+size(rawdata,1)-1),1:30,31:(31+size(rawdata,3)-1)) = rawdata(:,30:-1:1,:);% left padding
rawdataPadded(31:(31+size(rawdata,1)-1),size(rawdata,2)+31:end,31:(31+size(rawdata,3)-1)) = rawdata(:,end:-1:end-29,:);%right padding
rawdataPadded(1:30,:,31:(31+size(rawdata,3)-1)) = rawdataPadded(60:-1:31,:,31:(31+size(rawdata,3)-1));% top padding
rawdataPadded(end-29:end,:,31:(31+size(rawdata,3)-1)) = rawdataPadded(end-30:-1:end-59,:,31:(31+size(rawdata,3)-1));% bottom padding

rawdataPadded(:,:,1:30) = rawdataPadded(:,:,60:-1:31);% z top padding
rawdataPadded(:,:,end-29:end) = rawdataPadded(:,:,end-30:-1:end-59);% z bottom padding

rawdata = rawdataPadded;

%%

nx = x_rawdata;
ny = y_rawdata;
nz = z_rawdata;
disp(['Rawdata size = ', num2str(nx), ' x ' ,  num2str(ny), ' x ',  num2str(nz)]);
disp(['PSF size =     ', num2str(size(psf,1)), ' x ', num2str(size(psf,2)), ' x ', num2str(size(psf,3))]);
disp(['Subtracting background, ', num2str(background), ', from raw data.']);
rawdata = single(rawdata) - background;

% soften the x-y borders if requested
if nTapering > 0
    taperKernel = fspecial('gaussian', nTapering+30, nTapering);
    for iz=1:nz
        rawdata(:,:,iz) = edgetaper(rawdata(:,:,iz), taperKernel);
    end
end

deconvolved = deconvlucy(rawdata, psf, nIter);
deconvolved = deconvolved(31:(31+x_rawdata-1),31:(31+y_rawdata-1),31:(31+z_rawdata-1));

%%

if ischar(bSaveUint16)
    bSaveUint16 = logical(str2double(bSaveUint16));
end

if bSaveUint16
    deconvolved = uint16(deconvolved);
end
write3Dtiff_deflate(deconvolved, output_filepath);

toc
end
