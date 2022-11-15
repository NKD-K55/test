-- phpMyAdmin SQL Dump
-- version 5.1.1
-- https://www.phpmyadmin.net/
--
-- Máy chủ: 127.0.0.1
-- Thời gian đã tạo: Th6 11, 2022 lúc 04:27 PM
-- Phiên bản máy phục vụ: 10.4.22-MariaDB
-- Phiên bản PHP: 7.3.33

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Cơ sở dữ liệu: `db_shop`
--

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_category`
--

CREATE TABLE `db_category` (
  `id` int(11) NOT NULL,
  `name` varchar(255) CHARACTER SET utf8 NOT NULL,
  `link` varchar(255) CHARACTER SET utf8 NOT NULL,
  `level` int(2) NOT NULL,
  `parentid` int(11) NOT NULL,
  `orders` varchar(5) CHARACTER SET utf8 DEFAULT NULL,
  `created_at` datetime NOT NULL,
  `created_by` varchar(255) CHARACTER SET utf8 NOT NULL,
  `updated_at` datetime NOT NULL,
  `updated_by` varchar(255) CHARACTER SET utf8 NOT NULL,
  `trash` tinyint(1) NOT NULL DEFAULT 1,
  `status` tinyint(1) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_category`
--

INSERT INTO `db_category` (`id`, `name`, `link`, `level`, `parentid`, `orders`, `created_at`, `created_by`, `updated_at`, `updated_by`, `trash`, `status`) VALUES
(1, 'Bộ sản phẩm chăm da', 'bo-san-pham-cham-da', 2, 4, '0', '2022-05-17 00:00:00', '4', '2022-05-20 14:37:28', '1', 1, 1),
(2, 'Thuốc nhuộm tóc', 'thuoc-nhuom', 2, 3, '0', '2019-05-22 16:13:24', '4', '2019-05-22 22:37:28', '1', 1, 1),
(3, 'Chăm sóc tóc', 'cham-soc-toc', 1, 0, '0', '2022-05-17 04:53:56', '4', '2022-05-17 04:53:56', '4', 1, 1),
(4, 'Chăm sóc da mặt', 'cham-soc-da-mat', 1, 0, '1', '2022-05-17 04:56:37', '1', '2022-05-17 04:56:37', '1', 1, 1),
(5, 'Nước cân bằng da', 'nuoc-can-bang-da', 2, 4, '0', '2022-05-17 05:23:41', '1', '2022-05-11 21:33:41', '1', 1, 1),
(6, 'Kem chống nắng cho mặt', 'kem-chong-nang-cho-mat', 2, 4, '1', '2022-05-17 05:23:59', '1', '2022-05-20 02:09:26', '1', 1, 1),
(7, ' Mặt nạ', 'mat-na', 2, 3, '1', '2022-05-17 05:24:16', '1', '2022-05-17 05:24:16', '1', 1, 1),
(8, ' Tinh chất dưỡng', 'tinh-chat-duong', 2, 3, '2', '2019-06-03 21:39:48', '1', '2019-06-11 23:41:32', '4', 1, 1),
(9, ' Son môi', 'son-moi', 2, 10, '0', '2019-06-03 21:40:38', '1', '2022-05-20 14:36:44', '1', 1, 1),
(10, 'Trang điểm', 'trang-diem', 1, 0, '2', '2022-05-17 04:57:15', '1', '2022-05-17 04:57:15', '1', 1, 1),
(11, ' Phấn mắt', 'phan-mat', 2, 10, '4', '2019-06-03 22:37:59', '1', '2022-05-20 14:36:22', '1', 1, 1),
(12, 'Phấn má', 'phan-ma', 3, 5, '', '2019-06-11 23:02:48', '4', '2019-06-11 23:02:48', '4', 0, 0),
(13, 'Bông trang điểm', 'bong-trang-diem', 2, 10, '3', '2022-05-20 14:35:51', '1', '2022-05-20 14:35:51', '1', 1, 1);

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_config`
--

CREATE TABLE `db_config` (
  `id` int(11) NOT NULL,
  `mail_smtp` varchar(68) CHARACTER SET utf8 NOT NULL,
  `mail_smtp_password` varchar(100) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Password mail shop',
  `mail_noreply` varchar(68) CHARACTER SET utf8 NOT NULL,
  `priceShip` mediumtext CHARACTER SET utf8 NOT NULL,
  `title` varchar(100) CHARACTER SET utf8 NOT NULL,
  `description` text CHARACTER SET utf8 NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_config`
--

INSERT INTO `db_config` (`id`, `mail_smtp`, `mail_smtp_password`, `mail_noreply`, `priceShip`, `title`, `description`) VALUES
(1, 'sale.miuxinh.shop@gmail.com', '123456', 'vuthihop1210@gmail.com', '30000', 'Miuxinhshop', '');

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_contact`
--

CREATE TABLE `db_contact` (
  `id` int(11) NOT NULL,
  `title` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `phone` varchar(15) COLLATE utf8_unicode_ci NOT NULL,
  `email` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `content` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `created_at` text COLLATE utf8_unicode_ci NOT NULL,
  `status` int(11) NOT NULL DEFAULT 0,
  `trash` int(11) NOT NULL DEFAULT 1,
  `fullname` varchar(80) COLLATE utf8_unicode_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_contact`
--

INSERT INTO `db_contact` (`id`, `title`, `phone`, `email`, `content`, `created_at`, `status`, `trash`, `fullname`) VALUES
(1, 'Hay', '981643651', 'daigiadaklak@gmail.com', 'Hay', '2019/6/2', 1, 1, 'Ngô Văn Hiệp'),
(2, 'Shop', '981643651', 'vanhiepp19988@gmail.com', 'Shop 12 21', '2019/6/2', 1, 1, 'Ngô Văn Hiệp'),
(4, 'Câu hỏi', '090305078', 'luong@gmail.com', 'Thời giàn giao hang trong Quận 12 là bào nhiêu', '2019/6/2', 1, 1, 'Lương'),
(5, 'Câu hỏi', '090305078', 'luong@gmail.com', 'Thời giàn giao hang trong Quận 12 là bào nhiêu', '2019/6/2', 1, 0, 'Lương'),
(6, 'Yêu cầu', '90305078', 'luong@gmail.com', 'Chào shop', '2019/6/2', 1, 0, 'Lương'),
(7, 'Chào shop', '90305078', 'luong@gmail.com', '2', '2019/6/12', 0, 0, 'Lương'),
(9, 'Câu hỏi', '0981643651', 'hiep@gmail.com', ' Giá ship quận gò vấp bao nhiêu', '2019/6/12', 0, 1, 'Hiệp gà'),
(10, 'Câu hỏi', '0981643651', 'hiep@gmail.com', ' Giá ship quận gò vấp bao nhiêu', '2019/6/12', 1, 0, 'Hiệp gà');

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_content`
--

CREATE TABLE `db_content` (
  `id` int(11) NOT NULL,
  `title` varchar(255) CHARACTER SET utf8 NOT NULL,
  `alias` varchar(255) CHARACTER SET utf8 NOT NULL,
  `introtext` mediumtext CHARACTER SET utf8 NOT NULL,
  `fulltext` mediumtext CHARACTER SET utf8 NOT NULL,
  `img` varchar(255) CHARACTER SET utf8 NOT NULL,
  `created` datetime NOT NULL,
  `created_by` varchar(50) CHARACTER SET utf8 NOT NULL,
  `modified` datetime NOT NULL,
  `modified_by` varchar(50) CHARACTER SET utf8 NOT NULL,
  `trash` int(1) NOT NULL DEFAULT 1,
  `status` int(1) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_content`
--

INSERT INTO `db_content` (`id`, `title`, `alias`, `introtext`, `fulltext`, `img`, `created`, `created_by`, `modified`, `modified_by`, `trash`, `status`) VALUES
(4, 'Serum Caudalie Vinoperfect giúp ngăn ngừa vết thâm, chống lão hóa', 'serum-caudalie-vinoperfect-giup-ngan-ngua-vet-tham-chong-lao-hoa', '', '<ul>\r\n	<li>Serum Caudalie Vinoperfect gi&uacute;p ngăn ngừa vết th&acirc;m, chống l&atilde;o h&oacute;a</li>\r\n	<li><img alt=\"Serum Caudalie Vinoperfect giúp ngăn ngừa vết thâm, chống lão hóa\" src=\"http://localhost/baitaplon/upload/news/Serum-Caudalie-Vinoperfect.jpg\" />\r\n	<h2><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Thương hiệu Caudalie c&oacute; nổi tiếng kh&ocirc;ng?</a></h2>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Thị trường Mỹ phẩm năm 1993 trở n&ecirc;n s&ocirc;i động hơn với sự xuất hiện của thương hiệu Caudalie đến từ nước Ph&aacute;p. Người sử dụng c&oacute; những đ&aacute;nh gi&aacute; rất cao về chất lượng của sản phẩm đến từ thương hiệu đ&igrave;nh đ&aacute;m n&agrave;y.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Nh&agrave; s&aacute;ng lập thương hiệu Caudalie kh&ocirc;ng ai kh&aacute;c đ&oacute; ch&iacute;nh l&agrave; b&agrave; Mathilde Thomas &ndash; người kh&aacute;m ph&aacute; ra t&aacute;c dụng của những quả nho với l&agrave;n da kết hợp c&ugrave;ng gi&aacute;o sư Pr. Vercauteren &ndash; người được cấp bằng s&aacute;ng chế v&agrave; điều chế th&agrave;nh c&ocirc;ng tinh chất Vinoperfect từ nhựa nho.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Mỹ phẩm của thương hiệu Caudalie c&oacute; th&agrave;nh phần ch&iacute;nh từ những tr&aacute;i nho &ndash; loại tr&aacute;i c&acirc;y nổi tiếng tại nước Ph&aacute;p. Mathilde Thomas c&ugrave;ng chồng trồng nho, tham quan vườn nho v&agrave; cũng kh&aacute;m ph&aacute; được c&ocirc;ng dụng của loại tr&aacute;i c&acirc;y n&agrave;y đối với l&agrave;n da.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">B&ecirc;n trong th&agrave;nh phần của những quả nho c&oacute; chứa Polyphenol, Resveratrol, hoạt chất Viniferine, Vitamin E v&agrave; Omega 6,&hellip; mang đến nhiều c&ocirc;ng dụng bất ngờ trong chăm s&oacute;c da.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Thương hiệu Caudalie n&agrave;y hiện đang tập trung ph&aacute;t triển nhiều d&ograve;ng sản phẩm kh&aacute;c nhau như xịt kho&aacute;ng, kem chống nắng, son dưỡng m&ocirc;i, serum, tẩy tế b&agrave;o chết,&hellip; C&aacute;c sản phẩm của Caudalie c&oacute; chất lượng được đ&aacute;nh gi&aacute; rất cao.</a></p>\r\n\r\n	<h2><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Serum Caudalie Vinoperfect c&oacute; tốt kh&ocirc;ng?</a></h2>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">m&igrave;nh biết đến Serum Caudalie Vinoperfect qua sự giới thiệu của một người bạn đang l&agrave;m việc tại Ph&aacute;p. L&agrave;n da nhiều đốm n&acirc;u, t&agrave;n nhang, k&eacute;m sức sống của m&igrave;nh đ&atilde; được chăm s&oacute;c tốt hơn nhờ sử dụng em serum từ thương hiệu Caudalie. Serum Caudalie Vinoperfect được mệnh danh l&agrave; sản phẩm loại trừ, ngăn ngừa sạm da, đốm n&acirc;u tr&ecirc;n da h&agrave;ng đầu Thế Giới.</a></p>\r\n\r\n	<h3><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Th&agrave;nh phần của serum Caudalie &ldquo;thần th&aacute;nh&rdquo;</a></h3>\r\n\r\n	<table>\r\n		<tbody>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">AQUA (WATER)</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Nước</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">BUTYLENE GLYCOL</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">H&uacute;t ẩm (humectant)</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">GLYCERIN*</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Giữ ẩm</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">SQUALANE*</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Squalane từ dầu Olive</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">TOCOPHERYL ACETATE*</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Dẫn xuất Vitamin E</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">CETEARETH-20</a></td>\r\n				<td>\r\n				<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Chất hoạt động kh&ocirc;ng ph&acirc;n cực</a></p>\r\n\r\n				<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">&nbsp;</a></p>\r\n\r\n				<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Sử dụng nhiều trong c&aacute;c sản phẩm oil-in-water (nền nước)</a></p>\r\n				</td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">PALMITOYL GRAPEVINE SHOOT EXTRACT*</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Viniferine 1000</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">GLYCERYL STEARATE SE *</a></td>\r\n				<td>\r\n				<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Nhũ ho&aacute;&nbsp; từ thực vật</a></p>\r\n\r\n				<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">&nbsp;</a></p>\r\n\r\n				<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">(SE: self-emusifying) sử dụng trong sản phẩm oil-in-water (nền nước)</a></p>\r\n				</td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">BISABOLOL*</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Chiết xuất từ tinh dầu C&uacute;c la m&atilde;</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">PARFUM (FRAGRANCE)</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Hương liệu</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">CAPRYLYL GLYCOL</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Chất bảo quản</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">ACRYLATES/C10-30 ALKYL ACRYLATE CROSSPOLYMER</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Silicone base&nbsp;&nbsp;gi&uacute;p sản phẩm bền vững.</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">POTASSIUM SORBATE</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Chất bảo quản thực phẩm</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">XANTHAN GUM</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Chất l&agrave;m d&agrave;y</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">CARBOMER</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Chất hoạt động bề mặt</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">SODIUM HYDROXIDE</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">NaOH trong kết hợp với GLYCERYL STEARATE &nbsp;tự nhũ ho&aacute;</a></td>\r\n			</tr>\r\n			<tr>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">SODIUM PHYTATE</a></td>\r\n				<td><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Chất l&agrave;m đặc</a></td>\r\n			</tr>\r\n		</tbody>\r\n	</table>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Caudalie nổi tiếng với việc nghi&ecirc;n cứu th&agrave;nh c&ocirc;ng c&ocirc;ng dụng của những quả nho với việc chăm s&oacute;c da. Điều n&agrave;y khiến m&igrave;nh chắc chắn chiết xuất từ nho l&agrave; th&agrave;nh phần ch&iacute;nh ẩn chứa b&ecirc;n trong lọ Serum Caudalie Vinoperfect v&agrave; kết quả ho&agrave;n to&agrave;n ch&iacute;nh x&aacute;c.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Tinh chất ch&iacute;nh của serum l&agrave; Viniferine c&oacute; trong nhựa nho. Hoạt chất n&agrave;y c&oacute; c&ocirc;ng dụng c&acirc;n bằng sắc tố da, giảm t&igrave;nh trạng l&atilde;o h&oacute;a da, trị n&aacute;m ho&agrave;n hảo cho da. C&ocirc;ng dụng của Viniferine được cấp bằng s&aacute;ng chế v&agrave;o năm 2006 v&agrave; được nghi&ecirc;n cứu, ph&aacute;t triển đến hiện nay.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">B&ecirc;n trong th&agrave;nh phần của Serum Caudalie Vinoperfect ho&agrave;n to&agrave;n kh&ocirc;ng c&oacute; chứa h&oacute;a chất c&oacute; hại. Caudalie kh&ocirc;ng đưa v&agrave;o sản phẩm của m&igrave;nh Parabens, Sodium Laureth Sulfate c&ugrave;ng nhiều th&agrave;nh phần tổng hợp kh&aacute;c. C&oacute; thể n&oacute;i Serum Caudalie Vinoperfect v&ocirc; c&ugrave;ng l&agrave;nh t&iacute;nh về th&agrave;nh phần b&ecirc;n trong từng giọt serum.</a></p>\r\n\r\n	<h3><a href=\"http://localhost/baitaplon/index.php/news/index/6\">C&ocirc;ng dụng của serum trị n&aacute;m Caudalie Vinoperfect</a></h3>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Nhờ sự c&oacute; mặt của hoạt chất Viniferine b&ecirc;n trong th&agrave;nh phần m&agrave; serum mang đến nhiều c&ocirc;ng dụng chăm s&oacute;c da.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">C&ocirc;ng dụng của serum Caudalie Vinoperfect gi&uacute;p da:</a></p>\r\n\r\n	<ul>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/6\">K&iacute;ch th&iacute;ch qu&aacute; tr&igrave;nh tổng hợp, sản sinh ra collagen b&ecirc;n trong tế b&agrave;o da gi&uacute;p da c&oacute; độ đ&agrave;n hồi cao, tổng thể da săn chắc.</a></li>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Sắc tố da được ổn định hơn, hạn chế được những đốm sẫm m&agrave;u v&igrave; l&atilde;o h&oacute;a s&acirc;u b&ecirc;n trong da nhờ c&ocirc;ng dụng của th&agrave;nh phần đến từ nhựa quả nho.</a></li>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Chống lại qu&aacute; tr&igrave;nh oxy h&oacute;a tự nhi&ecirc;n của l&agrave;n da.</a></li>\r\n	</ul>\r\n\r\n	<h2><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Ưu &amp; nhược điểm của serum Caudalie</a></h2>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Xuất th&acirc;n từ thương hiệu đ&igrave;nh đ&aacute;m tr&ecirc;n Thế Giới, được mệnh danh l&agrave; sản phẩm chăm s&oacute;c da tốt số 1 tr&ecirc;n thị trường n&ecirc;n serum cũng sở hữu nhiều ưu điểm. Một số những ưu điểm nổi bật của em &yacute; phải kể đến như:</a></p>\r\n\r\n	<ul>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Thiết kế tinh tế, sắc sảo, nhỏ gọn dễ d&agrave;ng di chuyển, sử dụng tại nhiều nơi.</a></li>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Thẩm thấu nhanh v&agrave;o s&acirc;u b&ecirc;n trong da, kh&ocirc;ng g&acirc;y cảm gi&aacute;c bết d&iacute;nh kh&oacute; chịu.</a></li>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Th&agrave;nh phần l&agrave;nh t&iacute;nh với mọi loại da.</a></li>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Hiệu quả đến 95% khi sử dụng đ&uacute;ng hướng dẫn.</a></li>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Th&acirc;n thiện với con người v&agrave; m&ocirc;i trường.</a></li>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Kh&ocirc;ng chứa th&agrave;nh phần độc hại.</a></li>\r\n	</ul>\r\n\r\n	<h2><a href=\"http://localhost/baitaplon/index.php/news/index/6\">C&aacute;ch d&ugrave;ng serum Caudalie hiệu quả nhất</a></h2>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Serum mang đến hiệu quả cao khi sử dụng để skincare cho da l&agrave; điều kh&ocirc;ng thể phủ nhận. Tuy nhi&ecirc;n kh&ocirc;ng phải ai cũng biết c&aacute;ch sử dụng, tận hưởng ch&iacute;nh x&aacute;c c&ocirc;ng dụng m&agrave; serum mang đến. m&igrave;nh được hướng dẫn d&ugrave;ng serum với c&aacute;c bước ch&iacute;nh như sau:</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Sản phẩm n&agrave;y thẩm thấu rất nhanh v&igrave; em ấy l&agrave; nền nước, bạn h&atilde;y d&ugrave;ng 2 lần một ng&agrave;y v&agrave;o buổi s&aacute;ng v&agrave; tối.</a></p>\r\n\r\n	<ul>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Buổi s&aacute;ng sau khi d&ugrave;ng toner th&igrave; apply serum l&ecirc;n da để dưỡng.</a></li>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/6\">C&ograve;n buổi tối cũng tương tự, sau khi double cleanser xong th&igrave; d&ugrave;ng toner tiếp đến l&agrave; serum Caudalie Vinoperfect.</a></li>\r\n	</ul>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Buổi tối: Sau khi rửa mặt, d&ugrave;ng nước c&acirc;n bằng da v&agrave; d&ugrave;ng facial oil, m&igrave;nh cho ra ray 3 giọt Vinoperfect xoa l&ecirc;n đầu ng&oacute;n tay.</a></p>\r\n\r\n	<h2><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Đ&aacute;nh gi&aacute; bản th&acirc;n về em Serum Caudalie Vinoperfect</a></h2>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Như m&igrave;nh đ&atilde; n&oacute;i ở tr&ecirc;n th&igrave; m&igrave;nh thuộc tu&yacute;p da hỗn hợp thi&ecirc;n kh&ocirc;, da c&oacute; nhiều đốm sạm đen v&ocirc; c&ugrave;ng mất tự tin. Tuy nhi&ecirc;n sau khi được giới thiệu, tư vấn d&ugrave;ng Serum Caudalie Vinoperfect đ&atilde; khiến m&igrave;nh v&ocirc; c&ugrave;ng ngạc nhi&ecirc;n về kết quả nhận được. m&igrave;nh cũng đặc biệt h&agrave;i l&ograve;ng với những đặc điểm kh&aacute;c nhau của em serum đến từ thương hiệu Caudalie n&agrave;y.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Những điều l&agrave;m m&igrave;nh h&agrave;i l&ograve;ng ở em &yacute; c&oacute; thể kể đến như:</a></p>\r\n\r\n	<h3><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Thiết kế, m&ugrave;i hương, kết cấu</a></h3>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Tổng thể b&ecirc;n ngo&agrave;i em serum nắm bắt đ&uacute;ng xu hướng của thị trường hiện đại. serum được chứa trong lọ thủy tinh nhỏ, gọn g&agrave;ng, đảm bảo t&iacute;nh thẩm mỹ. Caudalie sử dụng đầu dropper để lấy serum, m&igrave;nh chỉ cần bấm nhẹ tr&ecirc;n đầu dropper đ&atilde; c&oacute; thể dễ d&agrave;ng lấy từng giọt nhỏ tinh chất để sử dụng.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">M&ugrave;i hương rất thơm do em n&agrave;y c&oacute; chứa hương liệu, cảm gi&aacute;c kh&aacute; giống m&ugrave;i nhựa c&acirc;y.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Texture của em Serum Caudalie Vinoperfect dạng serum lỏng.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Sắc trắng hơi đục của serum khiến m&igrave;nh nhận thấy serum kh&ocirc;ng sử dụng qu&aacute; nhiều chất tạo m&agrave;u để l&agrave;m n&ecirc;n sự thu h&uacute;t.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Từng giọt serum được nhỏ v&agrave;o l&ograve;ng b&agrave;n tay cho thấy r&otilde; sự nhẹ nh&agrave;ng, mềm mỏng khi thoa v&ocirc; c&ugrave;ng &ldquo;đ&atilde;&rdquo;.</a></p>\r\n\r\n	<h3><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Hiệu quả sử dụng em &yacute; đạt điểm 8/10</a></h3>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Sau 1 th&aacute;ng sử dụng Serum Caudalie Vinoperfect l&agrave;n da của m&igrave;nh tr&ocirc;ng ổn hơn rất nhiều. Tất nhi&ecirc;n m&igrave;nh c&oacute; sử dụng những d&ograve;ng mỹ phẩm chăm s&oacute;c da kh&ocirc; kh&aacute;c để n&iacute;u k&eacute;o m&igrave;nh xu&acirc;n cho l&agrave;n da của m&igrave;nh đấy ạ.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">C&ocirc;ng dụng thực của serum Caudalie Vinoperfect l&agrave; ngăn chặn c&aacute;c vết th&acirc;m, n&aacute;m,&hellip; l&agrave; ch&iacute;nh. 1 khoảng thời gian d&ugrave;ng cũng c&oacute; hiệu quả nhưng kh&ocirc;ng r&otilde; rệt như c&aacute;c d&ograve;ng đặc trị kh&aacute;c, kh&ocirc;ng c&oacute; độ mạnh tới mức &ldquo;repair/treat&rdquo; cho da, cho n&ecirc;n m&igrave;nh cũng khẳng định với mọi người em serum n&agrave;y chỉ c&oacute; t&aacute;c dụng dưỡng v&agrave; l&agrave;m tươi s&aacute;ng da th&ocirc;i chứ đặc trị c&aacute;c vấn đề về th&acirc;m n&aacute;m th&igrave; kh&ocirc;ng hiệu quả lắm.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">C&ocirc;ng dụng chống l&atilde;o h&oacute;a da th&igrave; m&igrave;nh nghĩ kh&ocirc;ng c&oacute; nhiều, đa phần c&aacute;c sản phẩm dưỡng tr&ecirc;n thị trường đều quảng b&aacute; chống l&atilde;o h&oacute;a nhưng da c&oacute; rất nhiều t&aacute;c nh&acirc;n g&acirc;y l&atilde;o h&oacute;a v&agrave; phải d&ugrave;ng sản phẩm l&acirc;u lắm mới thấy được hiệu quả, về phần n&agrave;y m&igrave;nh kh&ocirc;ng kỳ vọng nhiều.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">Chốt lại c&ocirc;ng dụng ch&iacute;nh: dưỡng da, l&agrave;m da tươi s&aacute;ng v&agrave; hạn chế c&aacute;c vấn đề n&aacute;m, xạm, th&acirc;m.</a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/6\">X&eacute;t về gi&aacute; th&agrave;nh th&igrave; kh&ocirc;ng qu&aacute; đắt, mức gi&aacute; n&agrave;y tầm trung đối với rất nhiều bạn n&ecirc;n ai th&iacute;ch th&igrave; mua d&ugrave;ng vẫn rất ok kh&ocirc;ng c&oacute; đăt đ&acirc;u.</a></p>\r\n	</li>\r\n</ul>\r\n\r\n<p>&nbsp;</p>\r\n', 'Serum-Caudalie-Vinoperfect.jpg', '2019-05-26 23:18:02', '1', '2022-05-17 04:25:40', '1', 1, 1),
(5, 'HongMeng OS của Huawei xuất hiện trực tuyến, có tên quốc tế là Ark OS', 'hongmeng-os-cua-huawei-xuat-hien-truc-tuyen-co-ten-quoc-te-la-ark-os', '', '<h2>Kh&ocirc;ng c&ograve;n nghi ngờ g&igrave; nữa,&nbsp;<a href=\"https://www.thegioididong.com/dtdd-huawei\" target=\"_blank\">Huawei</a>&nbsp;đang ph&aacute;t triển hệ điều h&agrave;nh của ri&ecirc;ng m&igrave;nh, c&oacute; thể được gọi l&agrave;&nbsp;<a href=\"https://www.thegioididong.com/tin-tuc/huawei-mate-30-se-chay-he-dieu-hanh-hongmeng-os-va-chip-kirin-985-1170811\" target=\"_blank\">HongMeng OS</a>&nbsp;ở Trung Quốc v&agrave; Ark OS ở c&aacute;c quốc gia kh&aacute;c. Hệ thống n&agrave;y đ&atilde; th&ocirc;ng qua chứng nhận nh&atilde;n hiệu của Văn ph&ograve;ng Thương hiệu Trung Quốc cũng như Văn ph&ograve;ng Sở hữu Tr&iacute; tuệ Ch&acirc;u &Acirc;u.</h2>\r\n\r\n<p>C&aacute;c b&aacute;o c&aacute;o gần đ&acirc;y cho thấy Huawei đ&atilde; nộp đơn xin cấp bằng s&aacute;ng chế giao diện&nbsp;<a href=\"https://www.thegioididong.com/dtdd\" target=\"_blank\">smartphone</a>&nbsp;với t&ecirc;n l&agrave; Huawei Ark OS.&nbsp;Trong đ&oacute;, Ark v&agrave; Huawei Ark đ&atilde; được&nbsp;Văn ph&ograve;ng nh&atilde;n hiệu v&agrave; bằng s&aacute;ng chế của Đức chứng nhận v&agrave;o th&aacute;ng 3 năm nay.</p>\r\n\r\n<p>Ứng dụng bằng s&aacute;ng chế cũng bao gồm một loạt c&aacute;c ảnh chụp m&agrave;n h&igrave;nh. Tuy nhi&ecirc;n, kh&ocirc;ng r&otilde; liệu ảnh chụp m&agrave;n h&igrave;nh b&ecirc;n dưới c&oacute; thuộc hệ thống HongMeng/ Ark sắp tới của Huawei hay kh&ocirc;ng.</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p><img alt=\"\" src=\"https://cdn.tgdd.vn/Files/2019/06/08/1171773/Gallery/ark-os-z.jpg\" style=\"height:309px; width:550px\" /></p>\r\n\r\n<p>Một trong những ảnh chụp m&agrave;n h&igrave;nh c&oacute; cụm từ &quot;Android Green Alliance&quot; gợi &yacute; rằng hệ thống HongMeng/ Ark c&oacute; thể tương th&iacute;ch với c&aacute;c ứng dụng&nbsp;<a href=\"https://www.thegioididong.com/dtdd?g=android\" target=\"_blank\">Android</a>. Li&ecirc;n minh Android Xanh được Huawei, Alibaba, Tencent v&agrave; Netease phối hợp ra mắt. N&oacute; nhằm mục đ&iacute;ch tạo ra một hệ sinh th&aacute;i ứng dụng an to&agrave;n, đ&aacute;ng tin cậy v&agrave; l&agrave;nh mạnh.</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p><img alt=\"\" src=\"https://cdn.tgdd.vn/Files/2019/06/08/1171773/ark-os_550x409.jpg\" style=\"height:409px; width:550px\" /></p>\r\n\r\n<p>Hiện nay, nhiều c&ocirc;ng ty đ&atilde; tham gia. Trước đ&acirc;y, Yu Chengdong -&nbsp;Gi&aacute;m đốc điều h&agrave;nh bộ phận kinh doanh ti&ecirc;u d&ugrave;ng của Huawei, đ&atilde; tiết lộ rằng Huawei OS sẽ tương th&iacute;ch với smartphone, m&aacute;y t&iacute;nh,&nbsp;<a href=\"https://www.thegioididong.com/may-tinh-bang\" target=\"_blank\">m&aacute;y t&iacute;nh bảng</a>, TV, xe hơi, thiết bị đeo th&ocirc;ng minh v&agrave; nhiều hệ thống kh&aacute;c.</p>\r\n\r\n<p>Nguồn:&nbsp;<a href=\"https://www.gizchina.com/2019/06/07/suspected-huawei-ark-os-interface-appears-online/\" target=\"_blank\">Gizchina</a></p>\r\n', '636944709585403075_huawei-dat-doanh-thu-khung-tren-tmall-cover.jpg', '2019-06-10 00:27:23', '1', '2019-06-12 22:53:05', '1', 0, 1),
(6, 'Nên thoa các sản phẩm chăm sóc da theo thứ tự nào?', 'nen-thoa-cac-san-pham-cham-soc-da-theo-thu-tu-nao', '', '<ul>\r\n	<li><span style=\"font-size:24px\"><strong><span style=\"background-color:#ffffff\">N&ecirc;n thoa c&aacute;c sản phẩm chăm s&oacute;c da theo thứ tự n&agrave;o?</span></strong></span></li>\r\n	<li><img alt=\"Nên thoa các sản phẩm chăm sóc da theo thứ tự nào?\" src=\"http://localhost/baitaplon/upload/news/CHAM-SOC-DA11.png\" />\r\n	<p>&nbsp;<a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Ch&uacute;ng t&ocirc;i đ&atilde; hỏi c&aacute;c chuy&ecirc;n gia về da c&acirc;u hỏi m&agrave; ch&uacute;ng ta lu&ocirc;n băn khoăn mỗi khi mua một sản phẩm chăm s&oacute;c da mới hoặc th&ecirc;m một bước v&agrave;o liệu tr&igrave;nh của m&igrave;nh: T&ocirc;i n&ecirc;n sử dụng c&aacute;c sản phẩm chăm s&oacute;c da của m&igrave;nh theo thứ tự n&agrave;o v&agrave; n&oacute; c&oacute; quan trọng kh&ocirc;ng? H&oacute;a ra l&agrave;, c&oacute;!</span></a></p>\r\n\r\n	<h3><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Tại sao thứ tự c&aacute;c bước chăm s&oacute;c da lại quan trọng</span></a><span style=\"background-color:#ffffff\">?</span></h3>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Việc sử dụng c&aacute;c sản phẩm chăm s&oacute;c da của bạn theo thứ tự ph&ugrave; hợp đảm bảo rằng l&agrave;n da nhận được đầy đủ tối đa hiệu quả từ mỗi sản phẩm.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Thứ tự của việc sử dụng l&agrave; v&ocirc; c&ugrave;ng quan trọng. C&ocirc;ng việc của l&agrave;n da l&agrave; ngăn cản mọi thứ, nhưng nhiều sản phẩm chăm s&oacute;c da ch&uacute;ng ta sử dụng c&oacute; những th&agrave;nh phần m&agrave; ch&uacute;ng ta muốn đưa v&agrave;o. Chỉ một lượng rất nhỏ c&aacute;c th&agrave;nh phần quan trọng n&agrave;y c&oacute; thể thẩm thấu v&agrave;o da, ngay cả khi được b&agrave;o chế v&agrave; thoa một c&aacute;ch ho&agrave;n hảo. Nếu bạn kh&ocirc;ng sử dụng c&aacute;c sản phẩm theo đ&uacute;ng thứ tự, bạn sẽ kh&ocirc;ng thấy được kết quả tốt nhất từ ​​chế độ chăm s&oacute;c da của m&igrave;nh.</span></a></p>\r\n\r\n	<h3><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Kiến thức cơ bản của bất kỳ liệu tr&igrave;nh chăm s&oacute;c da n&agrave;o</span></a></h3>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">B&ecirc;n cạnh thứ tự sử dụng, một số chuy&ecirc;n gia cho rằng việc c&acirc;n nhắc thời gian để da hấp thụ c&aacute;c sản phẩm của bạn cũng rất quan trọng. Tuy nhi&ecirc;n, v&igrave; hầu hết ch&uacute;ng ta thường vội v&atilde; để đi đ&acirc;u đ&oacute; (hay l&agrave;m việc kh&aacute;c), ch&uacute;ng t&ocirc;i hiểu rằng việc n&agrave;y kh&ocirc;ng phải l&uacute;c n&agrave;o cũng khả thi. Lời khuy&ecirc;n ở đ&acirc;y l&agrave; g&igrave;? Giữ cho liệu tr&igrave;nh chăm s&oacute;c da của bạn thật đơn giản.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Nhiều sản phẩm c&oacute; hướng dẫn rằng phải đợi từ 5 đến 30 ph&uacute;t trước khi thực hiện bước tiếp theo để đảm bảo hấp thụ tốt, nhưng mấy ai c&oacute; thời gian để thực hiện đủ? Nếu l&agrave;m cho liệu tr&igrave;nh trở n&ecirc;n qu&aacute; phức tạp, bạn c&oacute; thể sẽ kh&ocirc;ng đủ thời gian để thực hiện đủ c&aacute;c bước, Nếu liệu tr&igrave;nh của bạn c&oacute; nhiều bước, h&atilde;y tạm dừng một ch&uacute;t (một ph&uacute;t) để mỗi sản phẩm c&oacute; thời gian phản ứng với da. V&iacute; dụ, v&agrave;o buổi tối, sau khi rửa mặt v&agrave; thoa serum hay điều trị buổi tối, l&agrave; đủ, tiếp theo bạn chỉ cần kết th&uacute;c ng&agrave;y của m&igrave;nh v&agrave; leo l&ecirc;n giường. H&atilde;y để kem dưỡng ẩm tr&ecirc;n b&agrave;n cạnh giường v&agrave; thoa n&oacute; ngay trước khi ngủ để c&aacute;c liệu ph&aacute;p đặc trị (sản phẩm) thường đắt tiền n&agrave;y c&oacute; th&ecirc;m thời gian thẩm thấu v&agrave;o da trước khi bạn d&ugrave;ng kem dưỡng.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">C&oacute; lời khuy&ecirc;n n&agrave;o kh&aacute;c kh&ocirc;ng? H&atilde;y lắng nghe l&agrave;n da của bạn. V&agrave; nhớ rằng bạn l&agrave; duy nhất, kh&ocirc;ng ai kh&aacute;c c&oacute; l&agrave;n da giống của bạn. Bất kể c&aacute;c chuy&ecirc;n gia n&oacute;i g&igrave;, điều quan trọng l&agrave; phải t&igrave;m ra những g&igrave; ph&ugrave; hợp với bản th&acirc;n v&agrave; lu&ocirc;n đối xử tốt với l&agrave;n da của bạn.</span></a></p>\r\n\r\n	<h3><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">C&aacute;ch sử dụng c&aacute;c sản phẩm, v&agrave; tại sao thứ tự quan trọng</span></a></h3>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Vậy sắp xếp sản phẩm dựa tr&ecirc;n điều g&igrave;, bạn thắc mắc? H&atilde;y nhớ nguy&ecirc;n tắc ng&oacute;n tay c&aacute;i để &aacute;p dụng, thoa c&aacute;c sản phẩm của bạn theo đặc t&iacute;nh từ lo&atilde;ng nhất đến đặc nhất, hoặc từ dạng lỏng sang dạng kem. N&oacute;i chung, h&atilde;y nghĩ từ mỏng đến d&agrave;y, nhưng cũng h&atilde;y sử dụng c&aacute;c sản phẩm c&oacute; c&aacute;c th&agrave;nh phần quan trọng nhất để thẩm thấu v&agrave;o da (như chất chống oxy h&oacute;a trong serum) trước v&agrave; kết th&uacute;c bằng c&aacute;c sản phẩm cần thiệt cho da (như chất l&agrave;m mềm v&agrave; giữ ẩm trong kem dưỡng ẩm).</span></a></p>\r\n\r\n	<h3><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Liệu tr&igrave;nh chăm s&oacute;c da ban ng&agrave;y: hướng dẫn từng bước</span></a></h3>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Liệu tr&igrave;nh buổi s&aacute;ng nhằm để bảo vệ da khỏi &aacute;nh nắng mặt trời, &ocirc; nhiễm v&agrave; c&aacute;c yếu tố kh&aacute;c, v&igrave; vậy h&atilde;y để d&agrave;nh miếng lột da hay c&aacute;c sản phẩm điều trị, như retinol, v&agrave;o ban đ&ecirc;m.</span></a></p>\r\n\r\n	<h4><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">BƯỚC 1: L&Agrave;M SẠCH</span></a></h4>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">V&agrave;o buổi s&aacute;ng, h&atilde;y bắt đầu bằng c&aacute;ch sử dụng nước ấm l&ecirc;n mặt hoặc bằng sữa rửa mặt dịu nhẹ ph&ugrave; hợp với loại da của bạn, nếu cần.</span></a></p>\r\n\r\n	<h4><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">BƯỚC 2: TONER</span></a></h4>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Hầu hết mọi người chọn bỏ qua c&aacute;c loại toner, một phần v&igrave; c&oacute; nhiều người cho rằng hầu hết c&aacute;c loại toner trước đ&acirc;y đều c&oacute; nồng độ mạnh v&agrave; dễ g&acirc;y k&iacute;ch ứng da. May mắn thay, điều đ&oacute; kh&ocirc;ng c&ograve;n đ&uacute;ng với hiện tại nữa. Mặc d&ugrave; ch&uacute;ng kh&ocirc;ng &ldquo;thu nhỏ&rdquo; lỗ ch&acirc;n l&ocirc;ng về mặt vật l&yacute;, nhưng loại toner mới c&oacute; thể phục vụ nhiều mục đ&iacute;ch, hoạt động như một hệ thống ph&acirc;n phối chất chống oxy h&oacute;a, dẫn xuất vitamin B v&agrave; thậm ch&iacute; cả axit l&agrave;m săn da. Ngo&agrave;i ra, c&oacute; mỗi loại toner d&agrave;nh cho một vấn đề về da, v&igrave; vậy điều quan trọng l&agrave; bạn phải sử dụng đ&uacute;ng loại cho l&agrave;n da của m&igrave;nh.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Tuy nhi&ecirc;n, nếu đ&atilde; sống trọn đời m&agrave; kh&ocirc;ng sử dụng toner v&agrave; l&agrave;n da bạn vẫn tr&ocirc;ng khỏe mạnh, th&igrave; cũng kh&ocirc;ng cần phải bắt đầu sử dụng toner l&agrave;m g&igrave; cả. Toner được tạo ra để gi&uacute;p trả lại độ pH của da sau khi n&oacute; trở n&ecirc;n qu&aacute; mỏng manh v&igrave; c&aacute;c loại chất tẩy rửa mạnh như x&agrave; ph&ograve;ng. Nhưng sữa rửa mặt hiện nay đ&atilde; được c&acirc;n bằng tốt hơn rất nhiều n&ecirc;n kh&ocirc;ng phải d&ugrave;ng đến toner. Nhưng nếu c&oacute; một loại toner m&agrave; bạn th&iacute;ch sử dụng, th&igrave; việc gắn b&oacute; với n&oacute; cũng chẳng c&oacute; hại g&igrave; cả.</span></a></p>\r\n\r\n	<h4><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">BƯỚC 3: SERUM CHỐNG L&Atilde;O HO&Aacute;</span></a></h4>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Serum l&agrave; phương ph&aacute;p điều trị si&ecirc;u c&ocirc; đặc, gi&agrave;u dưỡng chất nhằm giải quyết c&aacute;c vấn đề cụ thể, v&igrave; vậy tốt hơn hết bạn n&ecirc;n b&ocirc;i ch&uacute;ng c&agrave;ng gần da c&agrave;ng tốt (lớp trong c&ugrave;ng). Hiện nay tr&ecirc;n thị trường c&oacute; rất nhiều loại serum kh&aacute;c nhau, nhưng đối với ban ng&agrave;y, ch&uacute;ng t&ocirc;i khuy&ecirc;n bạn n&ecirc;n sử dụng c&aacute;c loại serum chống oxy h&oacute;a, mang lại nhiều &iacute;ch lợi &mdash; từ l&agrave;m giảm phản ứng vi&ecirc;m của da đến trung h&ograve;a t&aacute;c hại từ tia UV v&agrave; c&aacute;c chất &ocirc; nhiễm m&ocirc;i trường.</span></a></p>\r\n\r\n	<h4><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">BƯỚC 4: KEM MẮT</span></a></h4>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Cần phải thoa kem dưỡng mắt &iacute;t nhất mỗi đ&ecirc;m &mdash; nếu kh&ocirc;ng thực hiện được hai lần một ng&agrave;y &mdash; bắt đầu từ độ tuổi 20. Đ&oacute; l&agrave; việc nhằm duy tr&igrave; sự khoẻ mạnh v&agrave; độ d&agrave;y của da m&iacute; mắt. Cải thiện chất lượng da v&ugrave;ng n&agrave;y sớm, để đảm bảo da m&iacute; mắt kh&ocirc;ng dễ bị ch&ugrave;ng nh&atilde;o v&agrave; mịn m&agrave;ng về sau n&agrave;y.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Để c&oacute; kết quả tốt nhất, t&iacute;nh ki&ecirc;n tr&igrave; nhất qu&aacute;n l&agrave; ch&igrave;a kh&oacute;a. &ldquo;Thường xuy&ecirc;n sử dụng kem dưỡng mắt theo thời gian sẽ giữ cho da m&iacute; mắt đ&agrave;n hồi v&agrave; cải thiện hay ngăn ngừa nếp nhăn hoặc mất collagen. Bạn c&oacute; thể bảo vệ th&ecirc;m v&ugrave;ng da mỏng manh quanh mắt bằng c&aacute;ch chọn kem dưỡng mắt c&oacute; SPF hay thoa kem chống nắng mỗi ng&agrave;y. Ch&uacute;ng t&ocirc;i cũng khuy&ecirc;n mọi người n&ecirc;n nhớ đeo k&iacute;nh r&acirc;m khi ra ngo&agrave;i để bảo vệ khỏi t&aacute;c hại của tia UV v&agrave; c&aacute;c đường c&oacute; thể len lỏi quanh kh&oacute;e mắt của bạn khi bạn nheo mắt.</span></a></p>\r\n\r\n	<h4><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">BƯỚC 5: ĐIỀU TRỊ TẠI CHỖ</span></a></h4>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">C&aacute;c loại thuốc điều trị k&ecirc; đơn v&agrave; phương ph&aacute;p điều trị mụn cần được thoa c&agrave;ng gần da c&agrave;ng tốt để ph&aacute;t huy tối đa hiệu quả của ch&uacute;ng.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">V&igrave; c&aacute;c phương ph&aacute;p điều trị c&aacute;c loại mụn kh&aacute;c nhau về th&agrave;nh phần hoạt t&iacute;nh, n&ecirc;n h&atilde;y đọc hướng dẫn sử dụng sản phẩm hay tham khảo &yacute; kiến b&aacute;c sĩ da liễu để sử dụng ch&uacute;ng một c&aacute;ch tốt nhất. Benzoyl peroxide, một th&agrave;nh phần trị mụn phổ biến, c&oacute; t&aacute;c dụng k&eacute;o d&agrave;i từ 1 đến 3 giờ. Bất kỳ loại kem n&agrave;o tiếp x&uacute;c với n&oacute; trước khi n&oacute; ho&agrave;n th&agrave;nh c&ocirc;ng việc, đều c&oacute; thể sẽ ngăn cản c&aacute;c hoạt chất hoạt động một c&aacute;ch b&igrave;nh thường.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">H&atilde;y nhớ rằng c&aacute;c phương ph&aacute;p điều trị mụn c&oacute; thể l&agrave;m kh&ocirc; da, v&igrave; vậy h&atilde;y lu&ocirc;n chỉ sử dụng tr&ecirc;n những điểm bạn cần.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">&nbsp;</span></a></p>\r\n\r\n	<h4><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">BƯỚC 6: DƯỠNG ẨM</span></a></h4>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Đ&uacute;ng vậy, tất cả mọi người đều cần một loại kem dưỡng ẩm, ngay cả khi c&oacute; l&agrave;n da dầu. Mặc d&ugrave; cơ thể bạn c&oacute; hệ thống b&ocirc;i trơn tự nhi&ecirc;n bao gồm c&aacute;c tuyến tiết dầu (hoặc b&atilde; nhờn) để duy tr&igrave; lớp bảo vệ chống lại c&aacute;c điều kiện khắc nghiệt b&ecirc;n ngo&agrave;i v&agrave; nhiễm tr&ugrave;ng, nhưng hầu hết ch&uacute;ng ta đều cần th&ecirc;m nước sau sự t&agrave;n ph&aacute; của &aacute;nh nắng mặt trời, thời tiết v&agrave; h&oacute;a chất g&acirc;y hại.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Hầu hết c&aacute;c chuy&ecirc;n gia đều khuy&ecirc;n rằng thời điểm tốt nhất để thoa kem dưỡng ẩm l&agrave; khi da vẫn c&ograve;n ẩm, v&igrave; vậy c&agrave;ng sớm thoa serum v&agrave; sản phẩm điều trị, bạn c&agrave;ng sớm thực hiện bước kh&oacute;a ẩm cần thiết c&ugrave;ng với kem dưỡng ẩm của m&igrave;nh.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Nếu đang trong qu&aacute; tr&igrave;nh điều trị mụn, bạn n&ecirc;n bỏ qua những v&ugrave;ng đang điều trị khi thoa kem dưỡng ẩm để đảm bảo c&aacute;c th&agrave;nh phần trong đ&oacute; sẽ kh&ocirc;ng ảnh hưởng đến c&aacute;c th&agrave;nh phần hoạt t&iacute;nh c&oacute; trong sản phẩm điều trị tại chỗ của bạn.</span></a></p>\r\n\r\n	<h4><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">BƯỚC 7: CHỐNG NẮNG</span></a></h4>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Kem chống nắng n&ecirc;n l&agrave; bước cuối c&ugrave;ng trong quy tr&igrave;nh chăm s&oacute;c da ban ng&agrave;y, nếu bạn đang sử dụng kem chống nắng vật l&yacute; hoặc kho&aacute;ng chất, hoạt động của ch&uacute;ng ngăn chặn c&aacute;c tia UV về mặt vật l&yacute;. Bước n&agrave;y sẽ trở n&ecirc;n phức tạp khi bạn sử dụng kem chống nắng h&oacute;a học.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Kem chống nắng h&oacute;a học phải được hấp thụ v&agrave;o da để c&oacute; hiệu quả, v&igrave; vậy thoa sau kem dưỡng ẩm sẽ tr&igrave; ho&atilde;n v&agrave; cản trở điều đ&oacute;. Tuy nhi&ecirc;n, nếu bạn thoa kem chống nắng h&oacute;a học trước kem dưỡng ẩm th&igrave; kem dưỡng ẩm của bạn lại sẽ kh&ocirc;ng ph&aacute;t huy hết t&aacute;c dụng v&igrave; da đ&atilde; được phủ một lớp kem chống nắng h&oacute;a học.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Để giải quyết vấn đề n&agrave;y, ch&uacute;ng t&ocirc;i khuy&ecirc;n bạn n&ecirc;n sử dụng kem chống nắng vật l&yacute; c&oacute; kẽm v&agrave; thoa sau kem dưỡng ẩm. &ldquo;Kẽm an to&agrave;n, hiệu quả v&agrave; cung cấp sự bảo vệ rộng nhất chống lại tia UVA v&agrave; UVB.&rdquo;</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Đối với những người th&iacute;ch sử dụng kem chống nắng h&oacute;a học, h&atilde;y cố gắng t&igrave;m kiếm một sản phẩm cung cấp c&aacute;c lợi &iacute;ch dưỡng ẩm, để bạn c&oacute; thể đ&aacute;p ứng nhu cầu hydrat h&oacute;a h&agrave;ng ng&agrave;y m&agrave; vẫn bảo vệ l&agrave;n da của m&igrave;nh.</span></a></p>\r\n\r\n	<h3><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Liệu tr&igrave;nh chăm s&oacute;c da ban đ&ecirc;m: hướng dẫn từng bước</span></a></h3>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Khi l&agrave;n da tự phục hồi một c&aacute;ch tự nhi&ecirc;n v&agrave;o ban đ&ecirc;m, liệu tr&igrave;nh chăm s&oacute;c da v&agrave;o ban đ&ecirc;m của bạn n&ecirc;n tập trung v&agrave;o việc điều trị v&agrave; cung cấp cho l&agrave;n da những g&igrave; n&oacute; cần. Nếu da bạn tr&ocirc;ng xỉn m&agrave;u, h&atilde;y tẩy tế b&agrave;o chết. Nếu n&oacute; bị k&iacute;ch ứng, h&atilde;y cấp nước v&agrave; bảo vệ. Dưới đ&acirc;y l&agrave; thứ tự c&aacute;c sản phẩm chăm s&oacute;c da tốt nhất để đảm bảo bạn tận dụng tối đa ch&uacute;ng:</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><img alt=\"Nên bôi các sản phẩm chăm sóc da theo thứ tự nào?\" src=\"https://veevee.store/wp-content/uploads/2022/02/skincare-nen-boi-cac-san-pham-skincare-theo-thu-tu-nao-4.jpeg\" style=\"height:450px; width:750px\" /></a></p>\r\n\r\n	<h4><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">BƯỚC 1: L&Agrave;M SẠCH (K&Eacute;P)</span></a></h4>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Để loại bỏ bụi bẩn, dầu v&agrave; lớp trang điểm trong ng&agrave;y, c&aacute;c chuy&ecirc;n gia khuy&ecirc;n bạn n&ecirc;n tẩy trang trước bằng sản phẩm tẩy trang chuy&ecirc;n dụng trước khi rửa mặt bằng sữa rửa mặt dịu nhẹ. Tốt hơn hết, h&atilde;y l&agrave;m sạch hai lần, bao gồm việc sử dụng dầu tẩy trang trước để h&ograve;a tan lớp trang điểm v&agrave; sau đ&oacute; rửa mặt lại bằng sữa rửa mặt th&ocirc;ng thường.</span></a></p>\r\n\r\n	<h4><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">BƯỚC 2: TONER, ESSENCE V&Agrave; BOOTER</span></a></h4>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Nếu bạn sử dụng toner, h&atilde;y thoa n&oacute; như bạn thường l&agrave;m v&agrave;o buổi s&aacute;ng.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">V&agrave;o ban đ&ecirc;m, một số người cũng th&iacute;ch phủ nhiều loại chất tăng cường chăm s&oacute;c da kh&aacute;c nhau, m&agrave; bạn thấy như xịt kho&aacute;ng, tinh chất, nước l&agrave;m đẹp hoặc serum dưỡng ẩm (hyaluronic acid). Ch&uacute;ng chứa c&aacute;c th&agrave;nh phần hoạt t&iacute;nh kh&aacute;c nhau, nhưng phần lớn, mục đ&iacute;ch chủ yếu l&agrave; cấp nước v&agrave; nu&ocirc;i dưỡng l&agrave;n da.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">V&igrave; đ&acirc;y l&agrave; những c&ocirc;ng thức nhẹ, gần giống như nước; Sử dụng ch&uacute;ng sau khi rửa mặt như đối với toner. Nếu sử dụng cả hai, h&atilde;y thoa toner sau đ&oacute; thoa essence. Toner l&agrave; để l&agrave;m sạch hơn v&agrave; essence để th&uacute;c đẩy hiệu quả cho c&aacute;c sản phẩm điều trị. Nếu sử dụng nhiều loại sản phẩm n&agrave;y, h&atilde;y &aacute;p dụng quy tắc tương tự: thoa từ mỏng đến d&agrave;y.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><img alt=\"Nên bôi các sản phẩm chăm sóc da theo thứ tự nào?\" src=\"https://veevee.store/wp-content/uploads/2022/02/skincare-nen-boi-cac-san-pham-skincare-theo-thu-tu-nao-5.jpeg\" style=\"height:450px; width:750px\" /></a></p>\r\n\r\n	<h4><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">BƯỚC 3: KEM MẮT</span></a></h4>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Ngo&agrave;i việc giải quyết c&aacute;c vết ch&acirc;n chim v&agrave; quầng th&acirc;m, kem dưỡng mắt c&ograve;n c&oacute; thể d&ugrave;ng để bảo vệ v&ugrave;ng da mắt mỏng manh của bạn khỏi c&aacute;c sản phẩm chăm s&oacute;c da kh&aacute;c.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">N&oacute;i chung, bạn n&ecirc;n thoa kem dưỡng mắt trước khi điều trị để bảo vệ v&ugrave;ng da mắt khỏi c&aacute;c th&agrave;nh phần mạnh c&oacute; thể g&acirc;y k&iacute;ch ứng.</span></a></p>\r\n\r\n	<h4><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">BƯỚC 4: ĐIỀU TRỊ, SERUM V&Agrave; PEEL</span></a></h4>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Giống như phần c&ograve;n lại tr&ecirc;n cơ thể, da thực hiện phần lớn c&ocirc;ng việc sửa chữa, phục hồi v&agrave; t&aacute;i tạo khi ch&uacute;ng ta ngủ. Đ&acirc;y l&agrave; l&yacute; do tại sao hầu hết c&aacute;c phương ph&aacute;p điều trị chăm s&oacute;c da được nhắm mục ti&ecirc;u v&agrave;o thời điểm n&agrave;y &mdash; như thuốc k&ecirc; đơn (tretinoin, kem trị mụn trứng c&aacute; v&agrave; bệnh rosacea), kem retinol, phương ph&aacute;p điều trị tẩy tế b&agrave;o chết (miếng lột v&agrave; mặt nạ) v&agrave; huyết thanh chống l&atilde;o h&oacute;a (truyền peptide, yếu tố tăng trưởng v&agrave; c&aacute;c th&agrave;nh phần hoạt t&iacute;nh sinh học kh&aacute;c) &mdash;Đều d&ugrave;ng tốt hơn v&agrave;o ban đ&ecirc;m. Tuy nhi&ecirc;n, thay v&igrave; tập trung tất cả c&aacute;c loại kem v&agrave; serum điều trị, ch&uacute;ng t&ocirc;i khuy&ecirc;n bạn n&ecirc;n chọn sản phẩm điều trị buổi tối t&ugrave;y thuộc v&agrave;o nhu cầu của l&agrave;n da, ph&ugrave; hợp với thời điểm hiện tại.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Qu&aacute; nhiều bước chỉ l&agrave;m tăng nguy cơ k&iacute;ch ứng v&agrave; giảm khả năng đạt được kết quả mong muốn. Lựa chọn phương ph&aacute;p điều trị v&agrave;o buổi tối n&ecirc;n dựa tr&ecirc;n những g&igrave; l&agrave;n da bạn cần v&agrave;o đ&ecirc;m h&ocirc;m đ&oacute;, kh&ocirc;ng dựa tr&ecirc;n những g&igrave; c&oacute; trong tủ mỹ phẩm của m&igrave;nh. Một số đ&ecirc;m, n&oacute; c&oacute; thể chỉ l&agrave; rửa mặt, dưỡng ẩm v&agrave; đi ngủ. Lu&ocirc;n lu&ocirc;n c&oacute; ng&agrave;y mai để sử dụng c&aacute;c sản phẩm kh&aacute;c, gi&uacute;p cho l&agrave;n da của bạn th&ecirc;m đ&aacute;ng y&ecirc;u.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Dưới đ&acirc;y l&agrave; một số lưu &yacute; kh&aacute;c:</span></a></p>\r\n\r\n	<ul>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Chọn giữa c&aacute;c loại thuốc k&ecirc; đơn (cho mụn hay bệnh trứng c&aacute; đỏ) HOẶC điều trị tẩy tế b&agrave;o chết (như miếng l&oacute;t AHA / BHA, miếng lột hoặc mặt nạ giải độc). Nếu bạn sử dụng cả hai trong c&ugrave;ng một đ&ecirc;m, bạn sẽ l&agrave;m tăng đ&aacute;ng kể nguy cơ k&iacute;ch ứng da.</span></a></li>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Kh&ocirc;ng sử dụng kem retinol (kh&ocirc;ng k&ecirc; đơn) v&agrave;o c&ugrave;ng đ&ecirc;m với sản phẩm tẩy da chết.</span></a></li>\r\n		<li><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">C&aacute;c sản phẩm tẩy tế b&agrave;o chết: Sử dụng c&aacute;c loại n&agrave;y một c&aacute;ch vừa phải, một đến ba lần một tuần.</span></a></li>\r\n	</ul>\r\n\r\n	<h4><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">BƯỚC 5: DƯỠNG ẨM HAY KEM BAN Đ&Ecirc;M</span></a></h4>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Một số người sử dụng c&ugrave;ng một loại dưỡng ẩm cho cả ng&agrave;y v&agrave; đ&ecirc;m. Tuy nhi&ecirc;n, dưỡng ẩm ban đ&ecirc;m hay kem dưỡng ẩm ban đ&ecirc;m thường d&agrave;y hơn, nặng hơn v&agrave; được thiết kế để hấp thụ trong v&agrave;i giờ.</span></a></p>\r\n\r\n	<p><a href=\"http://localhost/baitaplon/index.php/news/index/7\"><span style=\"background-color:#ffffff\">Kem dưỡng ẩm, đặc biệt l&agrave; những loại nặng hơn được sử dụng v&agrave;o ban đ&ecirc;m, tạo ra một lớp bảo vệ tr&ecirc;n da để ngăn chặn sự bốc hơi của nước trong khi bạn ngủ. Duy tr&igrave; h&agrave;m lượng nước cao trong da l&agrave; ch&igrave;a kh&oacute;a để chữa bệnh v&agrave; duy tr&igrave; l&agrave;n da khỏe mạnh. Nếu bạn đang sử dụng loại dưỡng ẩm ph&ugrave; hợp, đ&oacute; n&ecirc;n l&agrave; bước cuối c&ugrave;ng trong liệu tr&igrave;nh v&igrave; n&oacute; sẽ giữ kh&ocirc;ng c&oacute; g&igrave; vượt qua được cả. Nếu da bạn thuộc loại kh&ocirc;, bạn c&oacute; thể th&ecirc;m dầu dưỡng da mặt trước khi thoa kem dưỡng da ban đ&ecirc;m.</span></a>&nbsp;</p>\r\n	</li>\r\n</ul>\r\n', 'CHAM-SOC-DA11.png', '2019-06-26 17:22:04', '1', '2022-05-17 04:23:57', '1', 1, 1);
INSERT INTO `db_content` (`id`, `title`, `alias`, `introtext`, `fulltext`, `img`, `created`, `created_by`, `modified`, `modified_by`, `trash`, `status`) VALUES
(7, 'TOP 4 Kem chống nắng Vichy cho da thường, da dầu, hỗn hợp', 'top-4-kem-chong-nang-vichy-cho-da-thuong-da-dau-hon-hop', '', '<ul>\r\n	<li>\r\n	<p><span style=\"font-size:14px\"><span style=\"color:#000000\">TOP 6 Kem chống nắng Vichy cho da thường, da dầu, hỗn hợp</span></span></p>\r\n	</li>\r\n	<li>\r\n	<p><span style=\"font-size:14px\"><span style=\"color:#000000\"><img alt=\"TOP 6 Kem chống nắng Vichy cho da thường, da dầu, hỗn hợp\" src=\"http://localhost/baitaplon/upload/news/kem-chong-nang-vichy-ideal-soleil-ultra-melting-milk-Gel.jpg\" /></span></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">V&agrave;i n&eacute;t về thương hiệu kem chống nắng Vichy</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Vichy l&agrave; thương hiệu mỹ phẩm đến từ Ph&aacute;p, được th&agrave;nh lập từ năm 1931.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Với phương ch&acirc;m kh&ocirc;ng chỉ chăm s&oacute;c sắc đẹp m&agrave; c&ograve;n phải ch&uacute; trọng đến sức khỏe của l&agrave;n da, cải thiện để đem đến sự hiệu quả tốt nhất cho người d&ugrave;ng.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">V&igrave; vậy m&agrave; c&aacute;c sản phẩm đều được l&agrave;m từ những nguy&ecirc;n liệu an to&agrave;n v&agrave; dịu nhẹ đến từ thi&ecirc;n nhi&ecirc;n, nổi tiếng th&agrave;nh phần nước kho&aacute;ng l&agrave;m đẹp b&iacute; truyền h&agrave;ng ng&agrave;n tuổi.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Trong đ&oacute;, phải nhắc đến d&ograve;ng&nbsp;kem chống nắng Vichy&nbsp;với sự đột ph&aacute; lớn: gấp 3 m&agrave;ng lọc bảo vệ UV. Chi tiết thế n&agrave;o th&igrave; h&atilde;y c&ugrave;ng m&igrave;nh đi đến phần tiếp theo đ&acirc;y.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">C&oacute; những loại kem chống nắng Vichy n&agrave;o hiện nay?</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">1/ Kem chống nắng Vichy Ideal Soleil Mattifying Face Fluid Dry Touch cho da dầu</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Với những bạn da dầu th&igrave; kem chống nắng Vichy Ideal Soleil Mattifying Face Fluid Dry Touch lại l&agrave; sản phẩm ph&ugrave; hợp hơn cả, đ&acirc;y l&agrave; d&ograve;ng sản phẩm chuy&ecirc;n d&ugrave;ng cho da mặt n&ecirc;n chất kem kh&aacute; mỏng v&agrave; nhẹ.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Về thiết kế th&igrave; sản phẩm cũng kh&ocirc;ng thật sự nổi bật nhưng lại kh&aacute; chắc chắn, nhỏ gọn dễ d&agrave;ng mang theo khi đi ra ngo&agrave;i.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Điểm cộng đầu ti&ecirc;n d&agrave;nh cho &ldquo;em n&agrave;y&rdquo; l&agrave; được sản xuất dưới dạng essence hơi nghi&ecirc;ng về emulsion n&ecirc;n khi b&ocirc;i v&agrave;o da thẩm thấu rất nhanh.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Tuy nhi&ecirc;n v&igrave; kem l&agrave; sự kết hợp giữa vật l&yacute; v&agrave; h&oacute;a học n&ecirc;n kh&ocirc;ng c&oacute; t&aacute;c dụng n&acirc;ng tone da. B&ugrave; lại th&igrave; tr&ecirc;n da của bạn sẽ kh&ocirc;ng xuất hiện những vệt trắng.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Ri&ecirc;ng về chỉ số chống nắng th&igrave; sản phẩm c&oacute; chỉ số SPF 50 v&agrave; PA+++ n&ecirc;n được đ&aacute;nh gi&aacute; l&agrave; kh&aacute; ph&ugrave; hợp với thời tiết nắng n&oacute;ng tại Việt Nam. Đặc biệt, d&ograve;ng kem n&agrave;y c&ograve;n đem đến hiệu quả cao ngay cả khi da của bạn tiếp x&uacute;c dưới thời tiết 40 độ C.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Ưu điểm:</span></a></span></p>\r\n\r\n	<ul>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Lớp kem mềm c&oacute; t&aacute;c dụng thẩm thấu nhanh v&agrave;o da, kh&ocirc;ng tạo ra t&igrave;nh trạng bị bết d&iacute;nh v&agrave; cho cảm gi&aacute;c mịn m&agrave;ng</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Kh&ocirc;ng g&acirc;y ra t&igrave;nh trạng bị bệt da</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Cung cấp độ ẩm cho da, gi&uacute;p da th&ecirc;m mềm mại</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Sản phẩm th&iacute;ch hợp với nhiều loại da, ngay cả da nhạy cảm</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Rất th&iacute;ch hợp cho c&aacute;c chị em kh&ocirc;ng th&iacute;ch kem chống nắng g&acirc;y trắng da.</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Sản phẩm c&oacute; khả năng waterprof kh&aacute; tốt</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Bảo vệ da v&agrave; hỗ trợ t&aacute;i tạo da hiệu quả.</span></a></span></p>\r\n		</li>\r\n	</ul>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Nhược điểm:</span></a></span></p>\r\n\r\n	<ul>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Nắp hộp thiết kế chưa thật sự khoa học, phần kem sẽ bị d&iacute;nh tr&ecirc;n nắp hộp g&acirc;y oxy h&oacute;a v&agrave; mất vệ sinh.</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">V&igrave; kem thẩm thấu nhanh ch&oacute;ng n&ecirc;n bạn phải t&aacute;n nhanh nếu kh&ocirc;ng kem sẽ dễ bị tệp lại 1 chỗ.</span></a></span></p>\r\n		</li>\r\n	</ul>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">2/ Kem chống nắng Vichy Ideal Soleil 3 in 1 Tinted Anti-Dark Spots Care</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Kem chống nắng Vichy Ideal Soleil 3 in 1 Tinted Anti-Dark Spots Care l&agrave; d&ograve;ng sản phẩm chất lượng v&agrave; được nhiều kh&aacute;ch h&agrave;ng đ&aacute;nh gi&aacute; cao.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Cũng giống như Vichy Ideal Soleil Mattifying Face Fluid Dry Touch th&igrave; kem chống nắng n&agrave;y c&oacute; chỉ chỉ số SPF 50 mang đến cho bạn hiệu quả chống nắng kh&aacute; tốt.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Đ&acirc;y l&agrave; d&ograve;ng sản phẩm t&iacute;ch hợp 3 trong 1: nghĩa l&agrave; kem kh&ocirc;ng chỉ c&oacute; khả năng chống nắng tốt m&agrave; kem c&ograve;n c&oacute; t&aacute;c dụng l&agrave;m tăng tone da, chống nắng v&agrave; sử dụng như dạng kem nền BB cream</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Về kết cấu kem cũng kh&aacute; ổn, dễ thẩm thấu. Tuy nhi&ecirc;n sau một ng&agrave;y l&agrave;m việc th&igrave; chất kem hơi bị kh&ocirc; n&ecirc;n bạn cần tẩy trang kỹ để kh&ocirc;ng g&acirc;y t&igrave;nh trạng bị bệt da v&agrave; g&acirc;y tắc lỗ ch&acirc;n l&ocirc;ng, l&uacute;c đ&oacute; t&igrave;nh trạng mụn sẽ xuất hiện.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Nhờ phesocinol v&agrave; vitamin n&ecirc;n khi sử dụng kem th&igrave; da của bạn sẽ được cung cấp độ ẩm tốt n&ecirc;n bạn sẽ cảm gi&aacute;c được sự mềm mại, c&aacute;c th&agrave;nh phần n&agrave;y sẽ gi&uacute;p da kh&ocirc;ng bị mất nước v&agrave; giảm c&aacute;c vết sạm xuất hiện tr&ecirc;n da.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Đối với những bạn c&oacute; da dầu cũng n&ecirc;n tham khảo v&igrave; khả năng kiểm so&aacute;t dầu nhờn của da tương đối tốt nhờ chất Aluminium Starch Octenylsuccinate.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Đặc biệt, nước kho&aacute;ng Vichy sẽ gi&uacute;p da khỏe mạnh. N&oacute;i về khả năng chống nước th&igrave; Vichy Ideal Soleil 3 in 1 Tinted Anti-Dark Spots Care cũng kh&aacute; ổn. Tuy nhi&ecirc;n, nếu bạn tiếp x&uacute;c với nước trong thời gian d&agrave;i th&igrave; n&ecirc;n dặm lại kem nh&eacute;!</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Ưu điểm:</span></a></span></p>\r\n\r\n	<ul>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Kết cấu dạng kem tốt, dễ thẩm thấu v&agrave; c&oacute; t&aacute;c dụng tăng độ s&aacute;ng cho da khi b&ocirc;i.</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">C&oacute; thể sử dụng để l&agrave;m kem l&oacute;t hoặc thay thế bước kem nền khi trang điểm.</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Ngo&agrave;i t&aacute;c dụng chống nắng c&ograve;n sẽ hỗ trợ điều trị th&acirc;m n&aacute;m, sạm da một c&aacute;ch hiệu quả.</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Sản phẩm c&oacute; khả năng bảo vệ tốt cho da, tr&aacute;nh g&acirc;y ra những tổn thương kh&ocirc;ng đ&aacute;ng c&oacute; từ tia UV.</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">C&aacute;c th&agrave;nh phần c&oacute; trong sản phẩm kh&aacute; an to&agrave;n n&ecirc;n kh&ocirc;ng g&acirc;y ra t&igrave;nh trạng k&iacute;ch ứng da, d&ugrave; cho đ&oacute; l&agrave; da nhạy cảm nhất.</span></a></span></p>\r\n		</li>\r\n	</ul>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Nhược điểm:</span></a></span></p>\r\n\r\n	<ul>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Mặc d&ugrave; kem c&oacute; khả năng n&acirc;ng tone nhưng kh&ocirc;ng phải ai cũng ph&ugrave; hợp sử dụng.</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Kem kh&aacute; kh&ocirc; v&agrave; dễ tạo bợn. Do đ&oacute;, bạn n&ecirc;n t&aacute;n kỹ v&agrave; tẩy trang cẩn thận để kh&ocirc;ng g&acirc;y ra t&igrave;nh trạng bị bệt trắng v&agrave; g&acirc;y mụn khi chất kem c&ograve;n đọng cặn ở c&aacute;c lỗ ch&acirc;n l&ocirc;ng tr&ecirc;n da.</span></a></span></p>\r\n		</li>\r\n	</ul>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">3/ Xịt chống nắng Vichy Ideal Soleil Invisible Hydrating Mist Dry Touch</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Đ&acirc;y l&agrave; sản phẩm chống nắng nhưng theo dạng xịt, thiết kế cũng tiện để bạn mang theo b&ecirc;n m&igrave;nh v&agrave; sử dụng mỗi khi cần một c&aacute;ch nhanh ch&oacute;ng.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">V&igrave; sản phẩm l&agrave; dạng xịt phun sương n&ecirc;n chất kem sẽ c&oacute; kết cấu nhỏ li ti thẩm thấu rất nhanh v&agrave;o da. Kh&aacute;c hẳn những sản phẩm m&agrave; m&igrave;nh đề cập đến ở tr&ecirc;n, sản phẩm n&agrave;y được sản xuất dưới dạng kem chống nắng h&oacute;a học, bạn chỉ cần xịt đều tr&ecirc;n da l&agrave; c&oacute; thể tự tin ra ngo&agrave;i rồi.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Về chỉ số chống nắng th&igrave; sản phẩm c&oacute; chỉ số chống nắng SPF 50, PA+++ ph&ugrave; hợp ở m&ocirc;i trường Việt Nam.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Điểm nổi bật hơn hẳn c&aacute;c sản phẩm tr&ecirc;n l&agrave; sản phẩm c&oacute; quy tr&igrave;nh sản xuất 3 m&agrave;ng lọc độc quyền bảo vệ da tối đa khỏi những t&aacute;c nh&acirc;n g&acirc;y hại b&ecirc;n ngo&agrave;i. Kem chống nắng c&ograve;n c&oacute; chứa Butyrospermum Parkii đem đến m&ugrave;i hương dễ chịu khi sử dụng.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Đặc biệt l&agrave; d&ograve;ng xịt chống nắng n&agrave;y c&ograve;n c&oacute; Vitamin E ở dạng gần như nguy&ecirc;n bản c&oacute; t&aacute;c dụng dưỡng ẩm cho da, chống oxy h&oacute;a tối ưu nhất.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Nhưng bạn cũng n&ecirc;n lưu &yacute; đ&acirc;y l&agrave; d&ograve;ng sản phẩm dạng xịt n&ecirc;n đ&ocirc;i l&uacute;c bạn sẽ kh&ocirc;ng kiểm so&aacute;t được lượng kem tr&ecirc;n mặt. So với mặt bằng chung, d&ograve;ng kem xịt n&agrave;y c&oacute; gi&aacute; th&agrave;nh tương đối cao.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Ưu điểm:</span></a></span></p>\r\n\r\n	<ul>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Xịt v&agrave;o da thẩm thấu nhanh ch&oacute;ng v&agrave; c&oacute; cảm gi&aacute;c dễ chịu.</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Dễ d&agrave;ng xịt tới c&aacute;c v&ugrave;ng tr&ecirc;n cơ thể như sau lưng, sau m&ocirc;ng.</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Cung cấp độ ẩm cho da v&agrave; bảo vệ da một c&aacute;ch tốt nhất</span></a></span></p>\r\n		</li>\r\n	</ul>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Nhược điểm:</span></a></span></p>\r\n\r\n	<ul>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Kem được thiết kế dưới dạng dạng xịt khiến bạn kh&oacute; kiểm so&aacute;t lượng kem tr&ecirc;n da.</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Kh&ocirc;ng được ph&eacute;p mang l&ecirc;n m&aacute;y bay n&ecirc;n sẽ g&acirc;y bất tiện.</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Gi&aacute; sản phẩm cao so với c&aacute;c sản phẩm c&ugrave;ng thương hiệu</span></a></span></p>\r\n		</li>\r\n	</ul>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">4/ Kem chống nắng vichy Ideal Soleil Anti-Ageing chống l&atilde;o h&oacute;a</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Trong c&aacute;c d&ograve;ng kem chống nắng Vichy th&igrave; c&oacute; d&ograve;ng vichy Ideal Soleil Anti-Ageing ngo&agrave;i t&aacute;c dụng bảo vệ da khỏi tia UV c&ograve;n gi&uacute;p chống l&atilde;o h&oacute;a với sự c&oacute; mặt của tr&agrave; đen Kombucha.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Về thiết kế th&igrave; kh&ocirc;ng thật sự c&oacute; g&igrave; nổi bật cũng như chưa c&oacute; t&iacute;nh đột ph&aacute; so với những sản phẩm c&ugrave;ng thương hiệu kh&aacute;c m&agrave; m&igrave;nh đ&atilde; giới thiệu.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Cũng như những người anh em kh&aacute;c của m&igrave;nh th&igrave; d&ograve;ng kem chống n&agrave;y đ&atilde; ph&aacute;t huy sự th&agrave;nh c&ocirc;ng của tu&yacute;p Face Fluid giữa d&ograve;ng kem chống nắng vật l&yacute; v&agrave; chống nắng h&oacute;a học.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Kem c&oacute; kết cấu mỏng nhẹ, c&oacute; m&ugrave;i thơm nhưng kh&ocirc;ng gắt n&ecirc;n ho&agrave;n to&agrave;n ph&ugrave; hợp với những bạn c&oacute; l&agrave;n da nhạy cảm. V&igrave; vậy, bạn ho&agrave;n to&agrave;n c&oacute; thể sử dụng tr&ecirc;n mặt hoặc cả body.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Ngo&agrave;i ra, điểm nổi bật hơn cả l&agrave; kem c&oacute; chiết xuất từ thực vật Camu Camu c&oacute; chứa lượng vitamin C nhiều gấp 60 lần c&aacute;c loại cam th&ocirc;ng thường c&oacute; t&aacute;c dụng l&agrave;m s&aacute;ng da v&agrave; đồng thời hạn chế sự xuất hiện của sạm, n&aacute;m.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">N&oacute;i về khả năng chống nước th&igrave; em n&agrave;y kh&aacute; tốt n&ecirc;n bạn c&oacute; thể thoải m&aacute;i vận động m&agrave; kh&ocirc;ng sợ bị tr&ocirc;i lớp kem. Tuy nhi&ecirc;n, mức gi&aacute; của sản phẩm n&agrave;y kh&aacute; cao n&ecirc;n kh&ocirc;ng phải ai cũng c&oacute; đủ điều kiện kinh tế để tiếp cận được với sản phẩm.</span></a></span></p>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Ưu điểm</span></a></span></p>\r\n\r\n	<ul>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Kem đa t&aacute;c dụng, thực sự bảo vệ l&agrave;n da rất tốt dưới &aacute;nh nắng mặt trời v&agrave; chống l&atilde;o h&oacute;a.</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Chất kem mềm n&ecirc;n c&oacute; t&aacute;c dụng thẩm thấu nhanh v&agrave; cung cấp độ ẩm tốt cho da.</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Hỗ trợ l&agrave;m s&aacute;ng da.</span></a></span></p>\r\n		</li>\r\n	</ul>\r\n\r\n	<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Nhược điểm</span></a></span></p>\r\n\r\n	<ul>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Sản phẩm c&oacute; mức gi&aacute; cao hơn so với c&aacute;c d&ograve;ng sản phẩm kh&aacute;c</span></a></span></p>\r\n		</li>\r\n		<li>\r\n		<p><span style=\"font-size:14px\"><a href=\"http://localhost/baitaplon/index.php/news/index/5\"><span style=\"color:#000000\">Thiết kế kh&ocirc;ng thật sự nổi bật</span></a></span></p>\r\n		</li>\r\n	</ul>\r\n	</li>\r\n</ul>\r\n', 'kemnen.jpg', '2022-05-19 22:57:04', '1', '2022-05-19 22:57:04', '1', 1, 1);

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_customer`
--

CREATE TABLE `db_customer` (
  `id` int(11) NOT NULL,
  `fullname` varchar(100) CHARACTER SET utf8 NOT NULL,
  `username` varchar(100) CHARACTER SET utf8 NOT NULL,
  `password` varchar(32) CHARACTER SET utf8 NOT NULL,
  `address` varchar(100) CHARACTER SET utf8 NOT NULL,
  `phone` varchar(13) CHARACTER SET utf8 NOT NULL,
  `email` varchar(50) CHARACTER SET utf8 NOT NULL,
  `created` datetime NOT NULL,
  `trash` int(1) NOT NULL DEFAULT 1,
  `status` int(1) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_customer`
--

INSERT INTO `db_customer` (`id`, `fullname`, `username`, `password`, `address`, `phone`, `email`, `created`, `trash`, `status`) VALUES
(2, 'Hiệp gà', 'hiep1998', 'e10adc3949ba59abbe56e057f20f883e', '', '0981643651', 'vanhiepp1998@gmail.com', '2019-05-24 22:10:08', 1, 1),
(9, 'Nguyễn Đức Lương', 'luong1998', 'e10adc3949ba59abbe56e057f20f883e', '', '090932323', 'luong@gmail.com', '2019-06-11 17:15:12', 1, 1),
(39, 'Bảo bự', '', '', '', '052244141', 'baobu@gmail.com', '2019-06-16 23:19:45', 1, 1),
(46, 'Dương Minh Trí', '', '', '', '0908030405', '', '2019-06-22 23:11:42', 1, 1),
(47, 'Trần Thanh Tuy', '', '', '', '090305146', '', '2019-06-22 23:16:45', 1, 1),
(48, 'Xuân Thịnh', '', '', '', '090640316', '', '2019-06-25 22:52:27', 1, 1),
(49, 'Minh Sang', '', '', '', '0913164136', '', '2019-06-25 22:58:22', 1, 1),
(50, 'Hoàng Dũng', '', '', '', '096323646', '', '2019-06-25 23:09:57', 1, 1),
(51, 'Nguyễn Văn Trường', '', '', '', '0357646765', '', '2019-06-26 13:17:30', 1, 1),
(52, 'Hiệp Ngô', '', '', '', '076613166', '', '2019-06-26 16:26:19', 0, 1),
(53, 'Thanh Sơn', '', '', '', '0903121564', '', '2019-06-28 15:57:50', 0, 1),
(54, 'Thành', '', '', '', '033564616', '', '2019-06-28 22:17:44', 0, 1),
(55, 'Lương 21', '', '', '', '03056451', '', '2019-06-29 15:02:46', 0, 1),
(56, 'Tuấn Anh', '', '', '', '0305464844', '', '2019-06-29 15:09:06', 0, 1),
(57, 'Tiến Đạt', '', '', '', '032133234', '', '2019-07-05 23:01:51', 1, 1),
(66, 'Hiệp 2019', 'hiep2019', 'e10adc3949ba59abbe56e057f20f883e', '', '0306444854', 'vanhiepp19988@gmail.com', '2019-07-06 00:00:00', 1, 1),
(67, 'Lê Thị Nga', '', '', '', '0378706959', '', '2022-05-20 01:27:46', 1, 1),
(68, 'Vũ Hồng', '', '', '', '0358782665', '', '2022-05-22 01:31:49', 1, 1),
(69, 'HonaGF', 'hoandg', '25d55ad283aa400af464c76d713c07ad', '', '0746363533', 'haong@gmail.com', '2022-05-22 00:00:00', 1, 1),
(70, 'Vũ Thị hợp', '', '', '', '0378706959', '', '2022-05-22 20:03:24', 1, 1),
(71, 'Vũ Thị hợp', 'hopvttt', 'e10adc3949ba59abbe56e057f20f883e', '', '0368268252', 'hopvt1213@gmail.com', '2022-06-06 00:00:00', 1, 1),
(72, 'Lê Thị Nga', 'vuthihop', 'e10adc3949ba59abbe56e057f20f883e', '', '0368259264', 'hopvt01@gmail.com', '2022-06-06 00:00:00', 1, 1),
(73, 'ffsfsfdsf', '', '', '', '12313123', '', '2022-06-08 01:11:46', 0, 1),
(74, 'nga lê', 'ngalenga', 'e10adc3949ba59abbe56e057f20f883e', '', '0987564432', 'ngalele@gmail.com', '2022-06-11 00:00:00', 1, 1);

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_discount`
--

CREATE TABLE `db_discount` (
  `id` int(11) NOT NULL COMMENT 'ID',
  `code` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Mã giảm giá',
  `discount` int(11) NOT NULL COMMENT 'Số tiền',
  `limit_number` int(11) NOT NULL COMMENT 'giới hạn lượt mua',
  `number_used` int(11) NOT NULL COMMENT 'Số lượng đã nhập',
  `expiration_date` date NOT NULL COMMENT 'Ngày hết hạn',
  `payment_limit` int(11) NOT NULL COMMENT 'giới hạn đơn hàng tối thiểu',
  `description` varchar(255) COLLATE utf8_unicode_ci NOT NULL COMMENT 'Mô tả',
  `created` date NOT NULL,
  `orders` int(11) NOT NULL,
  `trash` int(1) NOT NULL,
  `status` int(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_discount`
--

INSERT INTO `db_discount` (`id`, `code`, `discount`, `limit_number`, `number_used`, `expiration_date`, `payment_limit`, `description`, `created`, `orders`, `trash`, `status`) VALUES
(1, 'MAHETLUOT', 100000, 30, 30, '2022-09-29', 500000, 'Giam 100000', '2019-06-10', 1, 1, 0),
(2, 'VANHIEP', 200000, 20, 3, '2019-06-12', 500000, 'Giam 200k', '2019-06-10', 1, 0, 1),
(3, 'VANHIEP123', 200000, 300, 103, '2022-07-01', 300000, 'giảm 200k', '2019-06-25', 1, 1, 1),
(4, 'MAHETHAN', 150000, 68, 23, '2022-06-30', 500000, 'giảm 300k', '2019-06-25', 1, 1, 0),
(13, 'CAOTHANG', 150000, 200, 1, '2022-06-20', 100000, 'Giảm 150k', '2019-07-06', 1, 1, 1),
(18, 'VSZ6GT9HFDYM', 100000, 1, 0, '2022-06-22', 0, 'Mã giảm giá 100.000 đ tự động khi đăng ký thành công', '2022-05-22', 0, 1, 1),
(19, 'T2HBEFQJ91WW', 100000, 1, 0, '2022-07-06', 0, 'Mã giảm giá 100.000 đ tự động khi đăng ký thành công', '2022-06-06', 0, 1, 1),
(20, 'REF0RHCKNHWM', 100000, 1, 0, '2022-07-06', 0, 'Mã giảm giá 100.000 đ tự động khi đăng ký thành công', '2022-06-06', 0, 1, 1),
(21, 'TMK9S4HGRC5A', 100000, 1, 0, '2022-07-11', 0, 'Mã giảm giá 100.000 đ tự động khi đăng ký thành công', '2022-06-11', 0, 1, 1);

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_district`
--

CREATE TABLE `db_district` (
  `id` int(5) NOT NULL,
  `name` varchar(100) CHARACTER SET utf8 NOT NULL,
  `type` varchar(30) CHARACTER SET utf8 NOT NULL,
  `provinceid` int(5) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_district`
--

INSERT INTO `db_district` (`id`, `name`, `type`, `provinceid`) VALUES
(1, 'Quận Ba Đình', 'Quận', 1),
(2, 'Quận Hoàn Kiếm', 'Quận', 1),
(3, 'Quận Tây Hồ', 'Quận', 1),
(4, 'Quận Long Biên', 'Quận', 1),
(5, 'Quận Cầu Giấy', 'Quận', 1),
(6, 'Quận Đống Đa', 'Quận', 1),
(7, 'Quận Hai Bà Trưng', 'Quận', 1),
(8, 'Quận Hoàng Mai', 'Quận', 1),
(9, 'Quận Thanh Xuân', 'Quận', 1),
(10, 'Huyện Sóc Sơn', 'Huyện', 1),
(11, 'Huyện Đông Anh', 'Huyện', 1),
(18, 'Huyện Gia Lâm', 'Huyện', 1),
(19, 'Quận Nam Từ Liêm', 'Quận', 1),
(20, 'Huyện Thanh Trì', 'Huyện', 1),
(21, 'Quận Bắc Từ Liêm', 'Quận', 1),
(24, 'Thành phố Hà Giang', 'Thành phố', 2),
(26, 'Huyện Đồng Văn', 'Huyện', 2),
(27, 'Huyện Mèo Vạc', 'Huyện', 2),
(28, 'Huyện Yên Minh', 'Huyện', 2),
(29, 'Huyện Quản Bạ', 'Huyện', 2),
(30, 'Huyện Vị Xuyên', 'Huyện', 2),
(31, 'Huyện Bắc Mê', 'Huyện', 2),
(32, 'Huyện Hoàng Su Phì', 'Huyện', 2),
(33, 'Huyện Xín Mần', 'Huyện', 2),
(34, 'Huyện Bắc Quang', 'Huyện', 2),
(35, 'Huyện Quang Bình', 'Huyện', 2),
(40, 'Thành phố Cao Bằng', 'Thành phố', 4),
(42, 'Huyện Bảo Lâm', 'Huyện', 4),
(43, 'Huyện Bảo Lạc', 'Huyện', 4),
(44, 'Huyện Thông Nông', 'Huyện', 4),
(45, 'Huyện Hà Quảng', 'Huyện', 4),
(46, 'Huyện Trà Lĩnh', 'Huyện', 4),
(47, 'Huyện Trùng Khánh', 'Huyện', 4),
(48, 'Huyện Hạ Lang', 'Huyện', 4),
(49, 'Huyện Quảng Uyên', 'Huyện', 4),
(50, 'Huyện Phục Hoà', 'Huyện', 4),
(51, 'Huyện Hoà An', 'Huyện', 4),
(52, 'Huyện Nguyên Bình', 'Huyện', 4),
(53, 'Huyện Thạch An', 'Huyện', 4),
(58, 'Thành Phố Bắc Kạn', 'Thành phố', 6),
(60, 'Huyện Pác Nặm', 'Huyện', 6),
(61, 'Huyện Ba Bể', 'Huyện', 6),
(62, 'Huyện Ngân Sơn', 'Huyện', 6),
(63, 'Huyện Bạch Thông', 'Huyện', 6),
(64, 'Huyện Chợ Đồn', 'Huyện', 6),
(65, 'Huyện Chợ Mới', 'Huyện', 6),
(66, 'Huyện Na Rì', 'Huyện', 6),
(70, 'Thành phố Tuyên Quang', 'Thành phố', 8),
(71, 'Huyện Lâm Bình', 'Huyện', 8),
(72, 'Huyện Nà Hang', 'Huyện', 8),
(73, 'Huyện Chiêm Hóa', 'Huyện', 8),
(74, 'Huyện Hàm Yên', 'Huyện', 8),
(75, 'Huyện Yên Sơn', 'Huyện', 8),
(76, 'Huyện Sơn Dương', 'Huyện', 8),
(80, 'Thành phố Lào Cai', 'Thành phố', 10),
(82, 'Huyện Bát Xát', 'Huyện', 10),
(83, 'Huyện Mường Khương', 'Huyện', 10),
(84, 'Huyện Si Ma Cai', 'Huyện', 10),
(85, 'Huyện Bắc Hà', 'Huyện', 10),
(86, 'Huyện Bảo Thắng', 'Huyện', 10),
(87, 'Huyện Bảo Yên', 'Huyện', 10),
(88, 'Huyện Sa Pa', 'Huyện', 10),
(89, 'Huyện Văn Bàn', 'Huyện', 10),
(94, 'Thành phố Điện Biên Phủ', 'Thành phố', 11),
(95, 'Thị Xã Mường Lay', 'Thị xã', 11),
(96, 'Huyện Mường Nhé', 'Huyện', 11),
(97, 'Huyện Mường Chà', 'Huyện', 11),
(98, 'Huyện Tủa Chùa', 'Huyện', 11),
(99, 'Huyện Tuần Giáo', 'Huyện', 11),
(100, 'Huyện Điện Biên', 'Huyện', 11),
(101, 'Huyện Điện Biên Đông', 'Huyện', 11),
(102, 'Huyện Mường Ảng', 'Huyện', 11),
(103, 'Huyện Nậm Pồ', 'Huyện', 11),
(105, 'Thành phố Lai Châu', 'Thành phố', 12),
(106, 'Huyện Tam Đường', 'Huyện', 12),
(107, 'Huyện Mường Tè', 'Huyện', 12),
(108, 'Huyện Sìn Hồ', 'Huyện', 12),
(109, 'Huyện Phong Thổ', 'Huyện', 12),
(110, 'Huyện Than Uyên', 'Huyện', 12),
(111, 'Huyện Tân Uyên', 'Huyện', 12),
(112, 'Huyện Nậm Nhùn', 'Huyện', 12),
(116, 'Thành phố Sơn La', 'Thành phố', 14),
(118, 'Huyện Quỳnh Nhai', 'Huyện', 14),
(119, 'Huyện Thuận Châu', 'Huyện', 14),
(120, 'Huyện Mường La', 'Huyện', 14),
(121, 'Huyện Bắc Yên', 'Huyện', 14),
(122, 'Huyện Phù Yên', 'Huyện', 14),
(123, 'Huyện Mộc Châu', 'Huyện', 14),
(124, 'Huyện Yên Châu', 'Huyện', 14),
(125, 'Huyện Mai Sơn', 'Huyện', 14),
(126, 'Huyện Sông Mã', 'Huyện', 14),
(127, 'Huyện Sốp Cộp', 'Huyện', 14),
(128, 'Huyện Vân Hồ', 'Huyện', 14),
(132, 'Thành phố Yên Bái', 'Thành phố', 15),
(133, 'Thị xã Nghĩa Lộ', 'Thị xã', 15),
(135, 'Huyện Lục Yên', 'Huyện', 15),
(136, 'Huyện Văn Yên', 'Huyện', 15),
(137, 'Huyện Mù Căng Chải', 'Huyện', 15),
(138, 'Huyện Trấn Yên', 'Huyện', 15),
(139, 'Huyện Trạm Tấu', 'Huyện', 15),
(140, 'Huyện Văn Chấn', 'Huyện', 15),
(141, 'Huyện Yên Bình', 'Huyện', 15),
(148, 'Thành phố Hòa Bình', 'Thành phố', 17),
(150, 'Huyện Đà Bắc', 'Huyện', 17),
(151, 'Huyện Kỳ Sơn', 'Huyện', 17),
(152, 'Huyện Lương Sơn', 'Huyện', 17),
(153, 'Huyện Kim Bôi', 'Huyện', 17),
(154, 'Huyện Cao Phong', 'Huyện', 17),
(155, 'Huyện Tân Lạc', 'Huyện', 17),
(156, 'Huyện Mai Châu', 'Huyện', 17),
(157, 'Huyện Lạc Sơn', 'Huyện', 17),
(158, 'Huyện Yên Thủy', 'Huyện', 17),
(159, 'Huyện Lạc Thủy', 'Huyện', 17),
(164, 'Thành phố Thái Nguyên', 'Thành phố', 19),
(165, 'Thành phố Sông Công', 'Thành phố', 19),
(167, 'Huyện Định Hóa', 'Huyện', 19),
(168, 'Huyện Phú Lương', 'Huyện', 19),
(169, 'Huyện Đồng Hỷ', 'Huyện', 19),
(170, 'Huyện Võ Nhai', 'Huyện', 19),
(171, 'Huyện Đại Từ', 'Huyện', 19),
(172, 'Thị xã Phổ Yên', 'Thị xã', 19),
(173, 'Huyện Phú Bình', 'Huyện', 19),
(178, 'Thành phố Lạng Sơn', 'Thành phố', 20),
(180, 'Huyện Tràng Định', 'Huyện', 20),
(181, 'Huyện Bình Gia', 'Huyện', 20),
(182, 'Huyện Văn Lãng', 'Huyện', 20),
(183, 'Huyện Cao Lộc', 'Huyện', 20),
(184, 'Huyện Văn Quan', 'Huyện', 20),
(185, 'Huyện Bắc Sơn', 'Huyện', 20),
(186, 'Huyện Hữu Lũng', 'Huyện', 20),
(187, 'Huyện Chi Lăng', 'Huyện', 20),
(188, 'Huyện Lộc Bình', 'Huyện', 20),
(189, 'Huyện Đình Lập', 'Huyện', 20),
(193, 'Thành phố Hạ Long', 'Thành phố', 22),
(194, 'Thành phố Móng Cái', 'Thành phố', 22),
(195, 'Thành phố Cẩm Phả', 'Thành phố', 22),
(196, 'Thành phố Uông Bí', 'Thành phố', 22),
(198, 'Huyện Bình Liêu', 'Huyện', 22),
(199, 'Huyện Tiên Yên', 'Huyện', 22),
(200, 'Huyện Đầm Hà', 'Huyện', 22),
(201, 'Huyện Hải Hà', 'Huyện', 22),
(202, 'Huyện Ba Chẽ', 'Huyện', 22),
(203, 'Huyện Vân Đồn', 'Huyện', 22),
(204, 'Huyện Hoành Bồ', 'Huyện', 22),
(205, 'Thị xã Đông Triều', 'Thị xã', 22),
(206, 'Thị xã Quảng Yên', 'Thị xã', 22),
(207, 'Huyện Cô Tô', 'Huyện', 22),
(213, 'Thành phố Bắc Giang', 'Thành phố', 24),
(215, 'Huyện Yên Thế', 'Huyện', 24),
(216, 'Huyện Tân Yên', 'Huyện', 24),
(217, 'Huyện Lạng Giang', 'Huyện', 24),
(218, 'Huyện Lục Nam', 'Huyện', 24),
(219, 'Huyện Lục Ngạn', 'Huyện', 24),
(220, 'Huyện Sơn Động', 'Huyện', 24),
(221, 'Huyện Yên Dũng', 'Huyện', 24),
(222, 'Huyện Việt Yên', 'Huyện', 24),
(223, 'Huyện Hiệp Hòa', 'Huyện', 24),
(227, 'Thành phố Việt Trì', 'Thành phố', 25),
(228, 'Thị xã Phú Thọ', 'Thị xã', 25),
(230, 'Huyện Đoan Hùng', 'Huyện', 25),
(231, 'Huyện Hạ Hoà', 'Huyện', 25),
(232, 'Huyện Thanh Ba', 'Huyện', 25),
(233, 'Huyện Phù Ninh', 'Huyện', 25),
(234, 'Huyện Yên Lập', 'Huyện', 25),
(235, 'Huyện Cẩm Khê', 'Huyện', 25),
(236, 'Huyện Tam Nông', 'Huyện', 25),
(237, 'Huyện Lâm Thao', 'Huyện', 25),
(238, 'Huyện Thanh Sơn', 'Huyện', 25),
(239, 'Huyện Thanh Thuỷ', 'Huyện', 25),
(240, 'Huyện Tân Sơn', 'Huyện', 25),
(243, 'Thành phố Vĩnh Yên', 'Thành phố', 26),
(244, 'Thị xã Phúc Yên', 'Thị xã', 26),
(246, 'Huyện Lập Thạch', 'Huyện', 26),
(247, 'Huyện Tam Dương', 'Huyện', 26),
(248, 'Huyện Tam Đảo', 'Huyện', 26),
(249, 'Huyện Bình Xuyên', 'Huyện', 26),
(250, 'Huyện Mê Linh', 'Huyện', 1),
(251, 'Huyện Yên Lạc', 'Huyện', 26),
(252, 'Huyện Vĩnh Tường', 'Huyện', 26),
(253, 'Huyện Sông Lô', 'Huyện', 26),
(256, 'Thành phố Bắc Ninh', 'Thành phố', 27),
(258, 'Huyện Yên Phong', 'Huyện', 27),
(259, 'Huyện Quế Võ', 'Huyện', 27),
(260, 'Huyện Tiên Du', 'Huyện', 27),
(261, 'Thị xã Từ Sơn', 'Thị xã', 27),
(262, 'Huyện Thuận Thành', 'Huyện', 27),
(263, 'Huyện Gia Bình', 'Huyện', 27),
(264, 'Huyện Lương Tài', 'Huyện', 27),
(268, 'Quận Hà Đông', 'Quận', 1),
(269, 'Thị xã Sơn Tây', 'Thị xã', 1),
(271, 'Huyện Ba Vì', 'Huyện', 1),
(272, 'Huyện Phúc Thọ', 'Huyện', 1),
(273, 'Huyện Đan Phượng', 'Huyện', 1),
(274, 'Huyện Hoài Đức', 'Huyện', 1),
(275, 'Huyện Quốc Oai', 'Huyện', 1),
(276, 'Huyện Thạch Thất', 'Huyện', 1),
(277, 'Huyện Chương Mỹ', 'Huyện', 1),
(278, 'Huyện Thanh Oai', 'Huyện', 1),
(279, 'Huyện Thường Tín', 'Huyện', 1),
(280, 'Huyện Phú Xuyên', 'Huyện', 1),
(281, 'Huyện Ứng Hòa', 'Huyện', 1),
(282, 'Huyện Mỹ Đức', 'Huyện', 1),
(288, 'Thành phố Hải Dương', 'Thành phố', 30),
(290, 'Thị xã Chí Linh', 'Thị xã', 30),
(291, 'Huyện Nam Sách', 'Huyện', 30),
(292, 'Huyện Kinh Môn', 'Huyện', 30),
(293, 'Huyện Kim Thành', 'Huyện', 30),
(294, 'Huyện Thanh Hà', 'Huyện', 30),
(295, 'Huyện Cẩm Giàng', 'Huyện', 30),
(296, 'Huyện Bình Giang', 'Huyện', 30),
(297, 'Huyện Gia Lộc', 'Huyện', 30),
(298, 'Huyện Tứ Kỳ', 'Huyện', 30),
(299, 'Huyện Ninh Giang', 'Huyện', 30),
(300, 'Huyện Thanh Miện', 'Huyện', 30),
(303, 'Quận Hồng Bàng', 'Quận', 31),
(304, 'Quận Ngô Quyền', 'Quận', 31),
(305, 'Quận Lê Chân', 'Quận', 31),
(306, 'Quận Hải An', 'Quận', 31),
(307, 'Quận Kiến An', 'Quận', 31),
(308, 'Quận Đồ Sơn', 'Quận', 31),
(309, 'Quận Dương Kinh', 'Quận', 31),
(311, 'Huyện Thuỷ Nguyên', 'Huyện', 31),
(312, 'Huyện An Dương', 'Huyện', 31),
(313, 'Huyện An Lão', 'Huyện', 31),
(314, 'Huyện Kiến Thuỵ', 'Huyện', 31),
(315, 'Huyện Tiên Lãng', 'Huyện', 31),
(316, 'Huyện Vĩnh Bảo', 'Huyện', 31),
(317, 'Huyện Cát Hải', 'Huyện', 31),
(318, 'Huyện Bạch Long Vĩ', 'Huyện', 31),
(323, 'Thành phố Hưng Yên', 'Thành phố', 33),
(325, 'Huyện Văn Lâm', 'Huyện', 33),
(326, 'Huyện Văn Giang', 'Huyện', 33),
(327, 'Huyện Yên Mỹ', 'Huyện', 33),
(328, 'Huyện Mỹ Hào', 'Huyện', 33),
(329, 'Huyện Ân Thi', 'Huyện', 33),
(330, 'Huyện Khoái Châu', 'Huyện', 33),
(331, 'Huyện Kim Động', 'Huyện', 33),
(332, 'Huyện Tiên Lữ', 'Huyện', 33),
(333, 'Huyện Phù Cừ', 'Huyện', 33),
(336, 'Thành phố Thái Bình', 'Thành phố', 34),
(338, 'Huyện Quỳnh Phụ', 'Huyện', 34),
(339, 'Huyện Hưng Hà', 'Huyện', 34),
(340, 'Huyện Đông Hưng', 'Huyện', 34),
(341, 'Huyện Thái Thụy', 'Huyện', 34),
(342, 'Huyện Tiền Hải', 'Huyện', 34),
(343, 'Huyện Kiến Xương', 'Huyện', 34),
(344, 'Huyện Vũ Thư', 'Huyện', 34),
(347, 'Thành phố Phủ Lý', 'Thành phố', 35),
(349, 'Huyện Duy Tiên', 'Huyện', 35),
(350, 'Huyện Kim Bảng', 'Huyện', 35),
(351, 'Huyện Thanh Liêm', 'Huyện', 35),
(352, 'Huyện Bình Lục', 'Huyện', 35),
(353, 'Huyện Lý Nhân', 'Huyện', 35),
(356, 'Thành phố Nam Định', 'Thành phố', 36),
(358, 'Huyện Mỹ Lộc', 'Huyện', 36),
(359, 'Huyện Vụ Bản', 'Huyện', 36),
(360, 'Huyện Ý Yên', 'Huyện', 36),
(361, 'Huyện Nghĩa Hưng', 'Huyện', 36),
(362, 'Huyện Nam Trực', 'Huyện', 36),
(363, 'Huyện Trực Ninh', 'Huyện', 36),
(364, 'Huyện Xuân Trường', 'Huyện', 36),
(365, 'Huyện Giao Thủy', 'Huyện', 36),
(366, 'Huyện Hải Hậu', 'Huyện', 36),
(369, 'Thành phố Ninh Bình', 'Thành phố', 37),
(370, 'Thành phố Tam Điệp', 'Thành phố', 37),
(372, 'Huyện Nho Quan', 'Huyện', 37),
(373, 'Huyện Gia Viễn', 'Huyện', 37),
(374, 'Huyện Hoa Lư', 'Huyện', 37),
(375, 'Huyện Yên Khánh', 'Huyện', 37),
(376, 'Huyện Kim Sơn', 'Huyện', 37),
(377, 'Huyện Yên Mô', 'Huyện', 37),
(380, 'Thành phố Thanh Hóa', 'Thành phố', 38),
(381, 'Thị xã Bỉm Sơn', 'Thị xã', 38),
(382, 'Thị xã Sầm Sơn', 'Thị xã', 38),
(384, 'Huyện Mường Lát', 'Huyện', 38),
(385, 'Huyện Quan Hóa', 'Huyện', 38),
(386, 'Huyện Bá Thước', 'Huyện', 38),
(387, 'Huyện Quan Sơn', 'Huyện', 38),
(388, 'Huyện Lang Chánh', 'Huyện', 38),
(389, 'Huyện Ngọc Lặc', 'Huyện', 38),
(390, 'Huyện Cẩm Thủy', 'Huyện', 38),
(391, 'Huyện Thạch Thành', 'Huyện', 38),
(392, 'Huyện Hà Trung', 'Huyện', 38),
(393, 'Huyện Vĩnh Lộc', 'Huyện', 38),
(394, 'Huyện Yên Định', 'Huyện', 38),
(395, 'Huyện Thọ Xuân', 'Huyện', 38),
(396, 'Huyện Thường Xuân', 'Huyện', 38),
(397, 'Huyện Triệu Sơn', 'Huyện', 38),
(398, 'Huyện Thiệu Hóa', 'Huyện', 38),
(399, 'Huyện Hoằng Hóa', 'Huyện', 38),
(400, 'Huyện Hậu Lộc', 'Huyện', 38),
(401, 'Huyện Nga Sơn', 'Huyện', 38),
(402, 'Huyện Như Xuân', 'Huyện', 38),
(403, 'Huyện Như Thanh', 'Huyện', 38),
(404, 'Huyện Nông Cống', 'Huyện', 38),
(405, 'Huyện Đông Sơn', 'Huyện', 38),
(406, 'Huyện Quảng Xương', 'Huyện', 38),
(407, 'Huyện Tĩnh Gia', 'Huyện', 38),
(412, 'Thành phố Vinh', 'Thành phố', 40),
(413, 'Thị xã Cửa Lò', 'Thị xã', 40),
(414, 'Thị xã Thái Hoà', 'Thị xã', 40),
(415, 'Huyện Quế Phong', 'Huyện', 40),
(416, 'Huyện Quỳ Châu', 'Huyện', 40),
(417, 'Huyện Kỳ Sơn', 'Huyện', 40),
(418, 'Huyện Tương Dương', 'Huyện', 40),
(419, 'Huyện Nghĩa Đàn', 'Huyện', 40),
(420, 'Huyện Quỳ Hợp', 'Huyện', 40),
(421, 'Huyện Quỳnh Lưu', 'Huyện', 40),
(422, 'Huyện Con Cuông', 'Huyện', 40),
(423, 'Huyện Tân Kỳ', 'Huyện', 40),
(424, 'Huyện Anh Sơn', 'Huyện', 40),
(425, 'Huyện Diễn Châu', 'Huyện', 40),
(426, 'Huyện Yên Thành', 'Huyện', 40),
(427, 'Huyện Đô Lương', 'Huyện', 40),
(428, 'Huyện Thanh Chương', 'Huyện', 40),
(429, 'Huyện Nghi Lộc', 'Huyện', 40),
(430, 'Huyện Nam Đàn', 'Huyện', 40),
(431, 'Huyện Hưng Nguyên', 'Huyện', 40),
(432, 'Thị xã Hoàng Mai', 'Thị xã', 40),
(436, 'Thành phố Hà Tĩnh', 'Thành phố', 42),
(437, 'Thị xã Hồng Lĩnh', 'Thị xã', 42),
(439, 'Huyện Hương Sơn', 'Huyện', 42),
(440, 'Huyện Đức Thọ', 'Huyện', 42),
(441, 'Huyện Vũ Quang', 'Huyện', 42),
(442, 'Huyện Nghi Xuân', 'Huyện', 42),
(443, 'Huyện Can Lộc', 'Huyện', 42),
(444, 'Huyện Hương Khê', 'Huyện', 42),
(445, 'Huyện Thạch Hà', 'Huyện', 42),
(446, 'Huyện Cẩm Xuyên', 'Huyện', 42),
(447, 'Huyện Kỳ Anh', 'Huyện', 42),
(448, 'Huyện Lộc Hà', 'Huyện', 42),
(449, 'Thị xã Kỳ Anh', 'Thị xã', 42),
(450, 'Thành Phố Đồng Hới', 'Thành phố', 44),
(452, 'Huyện Minh Hóa', 'Huyện', 44),
(453, 'Huyện Tuyên Hóa', 'Huyện', 44),
(454, 'Huyện Quảng Trạch', 'Thị xã', 44),
(455, 'Huyện Bố Trạch', 'Huyện', 44),
(456, 'Huyện Quảng Ninh', 'Huyện', 44),
(457, 'Huyện Lệ Thủy', 'Huyện', 44),
(458, 'Thị xã Ba Đồn', 'Huyện', 44),
(461, 'Thành phố Đông Hà', 'Thành phố', 45),
(462, 'Thị xã Quảng Trị', 'Thị xã', 45),
(464, 'Huyện Vĩnh Linh', 'Huyện', 45),
(465, 'Huyện Hướng Hóa', 'Huyện', 45),
(466, 'Huyện Gio Linh', 'Huyện', 45),
(467, 'Huyện Đa Krông', 'Huyện', 45),
(468, 'Huyện Cam Lộ', 'Huyện', 45),
(469, 'Huyện Triệu Phong', 'Huyện', 45),
(470, 'Huyện Hải Lăng', 'Huyện', 45),
(471, 'Huyện Cồn Cỏ', 'Huyện', 45),
(474, 'Thành phố Huế', 'Thành phố', 46),
(476, 'Huyện Phong Điền', 'Huyện', 46),
(477, 'Huyện Quảng Điền', 'Huyện', 46),
(478, 'Huyện Phú Vang', 'Huyện', 46),
(479, 'Thị xã Hương Thủy', 'Thị xã', 46),
(480, 'Thị xã Hương Trà', 'Thị xã', 46),
(481, 'Huyện A Lưới', 'Huyện', 46),
(482, 'Huyện Phú Lộc', 'Huyện', 46),
(483, 'Huyện Nam Đông', 'Huyện', 46),
(490, 'Quận Liên Chiểu', 'Quận', 48),
(491, 'Quận Thanh Khê', 'Quận', 48),
(492, 'Quận Hải Châu', 'Quận', 48),
(493, 'Quận Sơn Trà', 'Quận', 48),
(494, 'Quận Ngũ Hành Sơn', 'Quận', 48),
(495, 'Quận Cẩm Lệ', 'Quận', 48),
(497, 'Huyện Hòa Vang', 'Huyện', 48),
(498, 'Huyện Hoàng Sa', 'Huyện', 48),
(502, 'Thành phố Tam Kỳ', 'Thành phố', 49),
(503, 'Thành phố Hội An', 'Thành phố', 49),
(504, 'Huyện Tây Giang', 'Huyện', 49),
(505, 'Huyện Đông Giang', 'Huyện', 49),
(506, 'Huyện Đại Lộc', 'Huyện', 49),
(507, 'Thị xã Điện Bàn', 'Thị xã', 49),
(508, 'Huyện Duy Xuyên', 'Huyện', 49),
(509, 'Huyện Quế Sơn', 'Huyện', 49),
(510, 'Huyện Nam Giang', 'Huyện', 49),
(511, 'Huyện Phước Sơn', 'Huyện', 49),
(512, 'Huyện Hiệp Đức', 'Huyện', 49),
(513, 'Huyện Thăng Bình', 'Huyện', 49),
(514, 'Huyện Tiên Phước', 'Huyện', 49),
(515, 'Huyện Bắc Trà My', 'Huyện', 49),
(516, 'Huyện Nam Trà My', 'Huyện', 49),
(517, 'Huyện Núi Thành', 'Huyện', 49),
(518, 'Huyện Phú Ninh', 'Huyện', 49),
(519, 'Huyện Nông Sơn', 'Huyện', 49),
(522, 'Thành phố Quảng Ngãi', 'Thành phố', 51),
(524, 'Huyện Bình Sơn', 'Huyện', 51),
(525, 'Huyện Trà Bồng', 'Huyện', 51),
(526, 'Huyện Tây Trà', 'Huyện', 51),
(527, 'Huyện Sơn Tịnh', 'Huyện', 51),
(528, 'Huyện Tư Nghĩa', 'Huyện', 51),
(529, 'Huyện Sơn Hà', 'Huyện', 51),
(530, 'Huyện Sơn Tây', 'Huyện', 51),
(531, 'Huyện Minh Long', 'Huyện', 51),
(532, 'Huyện Nghĩa Hành', 'Huyện', 51),
(533, 'Huyện Mộ Đức', 'Huyện', 51),
(534, 'Huyện Đức Phổ', 'Huyện', 51),
(535, 'Huyện Ba Tơ', 'Huyện', 51),
(536, 'Huyện Lý Sơn', 'Huyện', 51),
(540, 'Thành phố Qui Nhơn', 'Thành phố', 52),
(542, 'Huyện An Lão', 'Huyện', 52),
(543, 'Huyện Hoài Nhơn', 'Huyện', 52),
(544, 'Huyện Hoài Ân', 'Huyện', 52),
(545, 'Huyện Phù Mỹ', 'Huyện', 52),
(546, 'Huyện Vĩnh Thạnh', 'Huyện', 52),
(547, 'Huyện Tây Sơn', 'Huyện', 52),
(548, 'Huyện Phù Cát', 'Huyện', 52),
(549, 'Thị xã An Nhơn', 'Thị xã', 52),
(550, 'Huyện Tuy Phước', 'Huyện', 52),
(551, 'Huyện Vân Canh', 'Huyện', 52),
(555, 'Thành phố Tuy Hoà', 'Thành phố', 54),
(557, 'Thị xã Sông Cầu', 'Thị xã', 54),
(558, 'Huyện Đồng Xuân', 'Huyện', 54),
(559, 'Huyện Tuy An', 'Huyện', 54),
(560, 'Huyện Sơn Hòa', 'Huyện', 54),
(561, 'Huyện Sông Hinh', 'Huyện', 54),
(562, 'Huyện Tây Hoà', 'Huyện', 54),
(563, 'Huyện Phú Hoà', 'Huyện', 54),
(564, 'Huyện Đông Hòa', 'Huyện', 54),
(568, 'Thành phố Nha Trang', 'Thành phố', 56),
(569, 'Thành phố Cam Ranh', 'Thành phố', 56),
(570, 'Huyện Cam Lâm', 'Huyện', 56),
(571, 'Huyện Vạn Ninh', 'Huyện', 56),
(572, 'Thị xã Ninh Hòa', 'Thị xã', 56),
(573, 'Huyện Khánh Vĩnh', 'Huyện', 56),
(574, 'Huyện Diên Khánh', 'Huyện', 56),
(575, 'Huyện Khánh Sơn', 'Huyện', 56),
(576, 'Huyện Trường Sa', 'Huyện', 56),
(582, 'Thành phố Phan Rang-Tháp Chàm', 'Thành phố', 58),
(584, 'Huyện Bác Ái', 'Huyện', 58),
(585, 'Huyện Ninh Sơn', 'Huyện', 58),
(586, 'Huyện Ninh Hải', 'Huyện', 58),
(587, 'Huyện Ninh Phước', 'Huyện', 58),
(588, 'Huyện Thuận Bắc', 'Huyện', 58),
(589, 'Huyện Thuận Nam', 'Huyện', 58),
(593, 'Thành phố Phan Thiết', 'Thành phố', 60),
(594, 'Thị xã La Gi', 'Thị xã', 60),
(595, 'Huyện Tuy Phong', 'Huyện', 60),
(596, 'Huyện Bắc Bình', 'Huyện', 60),
(597, 'Huyện Hàm Thuận Bắc', 'Huyện', 60),
(598, 'Huyện Hàm Thuận Nam', 'Huyện', 60),
(599, 'Huyện Tánh Linh', 'Huyện', 60),
(600, 'Huyện Đức Linh', 'Huyện', 60),
(601, 'Huyện Hàm Tân', 'Huyện', 60),
(602, 'Huyện Phú Quí', 'Huyện', 60),
(608, 'Thành phố Kon Tum', 'Thành phố', 62),
(610, 'Huyện Đắk Glei', 'Huyện', 62),
(611, 'Huyện Ngọc Hồi', 'Huyện', 62),
(612, 'Huyện Đắk Tô', 'Huyện', 62),
(613, 'Huyện Kon Plông', 'Huyện', 62),
(614, 'Huyện Kon Rẫy', 'Huyện', 62),
(615, 'Huyện Đắk Hà', 'Huyện', 62),
(616, 'Huyện Sa Thầy', 'Huyện', 62),
(617, 'Huyện Tu Mơ Rông', 'Huyện', 62),
(618, 'Huyện Ia H\' Drai', 'Huyện', 62),
(622, 'Thành phố Pleiku', 'Thành phố', 64),
(623, 'Thị xã An Khê', 'Thị xã', 64),
(624, 'Thị xã Ayun Pa', 'Thị xã', 64),
(625, 'Huyện KBang', 'Huyện', 64),
(626, 'Huyện Đăk Đoa', 'Huyện', 64),
(627, 'Huyện Chư Păh', 'Huyện', 64),
(628, 'Huyện Ia Grai', 'Huyện', 64),
(629, 'Huyện Mang Yang', 'Huyện', 64),
(630, 'Huyện Kông Chro', 'Huyện', 64),
(631, 'Huyện Đức Cơ', 'Huyện', 64),
(632, 'Huyện Chư Prông', 'Huyện', 64),
(633, 'Huyện Chư Sê', 'Huyện', 64),
(634, 'Huyện Đăk Pơ', 'Huyện', 64),
(635, 'Huyện Ia Pa', 'Huyện', 64),
(637, 'Huyện Krông Pa', 'Huyện', 64),
(638, 'Huyện Phú Thiện', 'Huyện', 64),
(639, 'Huyện Chư Pưh', 'Huyện', 64),
(643, 'Thành phố Buôn Ma Thuột', 'Thành phố', 66),
(644, 'Thị Xã Buôn Hồ', 'Thị xã', 66),
(645, 'Huyện Ea H\'leo', 'Huyện', 66),
(646, 'Huyện Ea Súp', 'Huyện', 66),
(647, 'Huyện Buôn Đôn', 'Huyện', 66),
(648, 'Huyện Cư M\'gar', 'Huyện', 66),
(649, 'Huyện Krông Búk', 'Huyện', 66),
(650, 'Huyện Krông Năng', 'Huyện', 66),
(651, 'Huyện Ea Kar', 'Huyện', 66),
(652, 'Huyện M\'Đrắk', 'Huyện', 66),
(653, 'Huyện Krông Bông', 'Huyện', 66),
(654, 'Huyện Krông Pắc', 'Huyện', 66),
(655, 'Huyện Krông A Na', 'Huyện', 66),
(656, 'Huyện Lắk', 'Huyện', 66),
(657, 'Huyện Cư Kuin', 'Huyện', 66),
(660, 'Thị xã Gia Nghĩa', 'Thị xã', 67),
(661, 'Huyện Đăk Glong', 'Huyện', 67),
(662, 'Huyện Cư Jút', 'Huyện', 67),
(663, 'Huyện Đắk Mil', 'Huyện', 67),
(664, 'Huyện Krông Nô', 'Huyện', 67),
(665, 'Huyện Đắk Song', 'Huyện', 67),
(666, 'Huyện Đắk R Lấp', 'Huyện', 67),
(667, 'Huyện Tuy Đức', 'Huyện', 67),
(672, 'Thành phố Đà Lạt', 'Thành phố', 68),
(673, 'Thành phố Bảo Lộc', 'Thành phố', 68),
(674, 'Huyện Đam Rông', 'Huyện', 68),
(675, 'Huyện Lạc Dương', 'Huyện', 68),
(676, 'Huyện Lâm Hà', 'Huyện', 68),
(677, 'Huyện Đơn Dương', 'Huyện', 68),
(678, 'Huyện Đức Trọng', 'Huyện', 68),
(679, 'Huyện Di Linh', 'Huyện', 68),
(680, 'Huyện Bảo Lâm', 'Huyện', 68),
(681, 'Huyện Đạ Huoai', 'Huyện', 68),
(682, 'Huyện Đạ Tẻh', 'Huyện', 68),
(683, 'Huyện Cát Tiên', 'Huyện', 68),
(688, 'Thị xã Phước Long', 'Thị xã', 70),
(689, 'Thị xã Đồng Xoài', 'Thị xã', 70),
(690, 'Thị xã Bình Long', 'Thị xã', 70),
(691, 'Huyện Bù Gia Mập', 'Huyện', 70),
(692, 'Huyện Lộc Ninh', 'Huyện', 70),
(693, 'Huyện Bù Đốp', 'Huyện', 70),
(694, 'Huyện Hớn Quản', 'Huyện', 70),
(695, 'Huyện Đồng Phú', 'Huyện', 70),
(696, 'Huyện Bù Đăng', 'Huyện', 70),
(697, 'Huyện Chơn Thành', 'Huyện', 70),
(698, 'Huyện Phú Riềng', 'Huyện', 70),
(703, 'Thành phố Tây Ninh', 'Thành phố', 72),
(705, 'Huyện Tân Biên', 'Huyện', 72),
(706, 'Huyện Tân Châu', 'Huyện', 72),
(707, 'Huyện Dương Minh Châu', 'Huyện', 72),
(708, 'Huyện Châu Thành', 'Huyện', 72),
(709, 'Huyện Hòa Thành', 'Huyện', 72),
(710, 'Huyện Gò Dầu', 'Huyện', 72),
(711, 'Huyện Bến Cầu', 'Huyện', 72),
(712, 'Huyện Trảng Bàng', 'Huyện', 72),
(718, 'Thành phố Thủ Dầu Một', 'Thành phố', 74),
(719, 'Huyện Bàu Bàng', 'Huyện', 74),
(720, 'Huyện Dầu Tiếng', 'Huyện', 74),
(721, 'Thị xã Bến Cát', 'Thị xã', 74),
(722, 'Huyện Phú Giáo', 'Huyện', 74),
(723, 'Thị xã Tân Uyên', 'Thị xã', 74),
(724, 'Thị xã Dĩ An', 'Thị xã', 74),
(725, 'Thị xã Thuận An', 'Thị xã', 74),
(726, 'Huyện Bắc Tân Uyên', 'Huyện', 74),
(731, 'Thành phố Biên Hòa', 'Thành phố', 75),
(732, 'Thị xã Long Khánh', 'Thị xã', 75),
(734, 'Huyện Tân Phú', 'Huyện', 75),
(735, 'Huyện Vĩnh Cửu', 'Huyện', 75),
(736, 'Huyện Định Quán', 'Huyện', 75),
(737, 'Huyện Trảng Bom', 'Huyện', 75),
(738, 'Huyện Thống Nhất', 'Huyện', 75),
(739, 'Huyện Cẩm Mỹ', 'Huyện', 75),
(740, 'Huyện Long Thành', 'Huyện', 75),
(741, 'Huyện Xuân Lộc', 'Huyện', 75),
(742, 'Huyện Nhơn Trạch', 'Huyện', 75),
(747, 'Thành phố Vũng Tàu', 'Thành phố', 77),
(748, 'Thành phố Bà Rịa', 'Thành phố', 77),
(750, 'Huyện Châu Đức', 'Huyện', 77),
(751, 'Huyện Xuyên Mộc', 'Huyện', 77),
(752, 'Huyện Long Điền', 'Huyện', 77),
(753, 'Huyện Đất Đỏ', 'Huyện', 77),
(754, 'Huyện Tân Thành', 'Huyện', 77),
(755, 'Huyện Côn Đảo', 'Huyện', 77),
(760, 'Quận 1', 'Quận', 79),
(761, 'Quận 12', 'Quận', 79),
(762, 'Quận Thủ Đức', 'Quận', 79),
(763, 'Quận 9', 'Quận', 79),
(764, 'Quận Gò Vấp', 'Quận', 79),
(765, 'Quận Bình Thạnh', 'Quận', 79),
(766, 'Quận Tân Bình', 'Quận', 79),
(767, 'Quận Tân Phú', 'Quận', 79),
(768, 'Quận Phú Nhuận', 'Quận', 79),
(769, 'Quận 2', 'Quận', 79),
(770, 'Quận 3', 'Quận', 79),
(771, 'Quận 10', 'Quận', 79),
(772, 'Quận 11', 'Quận', 79),
(773, 'Quận 4', 'Quận', 79),
(774, 'Quận 5', 'Quận', 79),
(775, 'Quận 6', 'Quận', 79),
(776, 'Quận 8', 'Quận', 79),
(777, 'Quận Bình Tân', 'Quận', 79),
(778, 'Quận 7', 'Quận', 79),
(783, 'Huyện Củ Chi', 'Huyện', 79),
(784, 'Huyện Hóc Môn', 'Huyện', 79),
(785, 'Huyện Bình Chánh', 'Huyện', 79),
(786, 'Huyện Nhà Bè', 'Huyện', 79),
(787, 'Huyện Cần Giờ', 'Huyện', 79),
(794, 'Thành phố Tân An', 'Thành phố', 80),
(795, 'Thị xã Kiến Tường', 'Thị xã', 80),
(796, 'Huyện Tân Hưng', 'Huyện', 80),
(797, 'Huyện Vĩnh Hưng', 'Huyện', 80),
(798, 'Huyện Mộc Hóa', 'Huyện', 80),
(799, 'Huyện Tân Thạnh', 'Huyện', 80),
(800, 'Huyện Thạnh Hóa', 'Huyện', 80),
(801, 'Huyện Đức Huệ', 'Huyện', 80),
(802, 'Huyện Đức Hòa', 'Huyện', 80),
(803, 'Huyện Bến Lức', 'Huyện', 80),
(804, 'Huyện Thủ Thừa', 'Huyện', 80),
(805, 'Huyện Tân Trụ', 'Huyện', 80),
(806, 'Huyện Cần Đước', 'Huyện', 80),
(807, 'Huyện Cần Giuộc', 'Huyện', 80),
(808, 'Huyện Châu Thành', 'Huyện', 80),
(815, 'Thành phố Mỹ Tho', 'Thành phố', 82),
(816, 'Thị xã Gò Công', 'Thị xã', 82),
(817, 'Thị xã Cai Lậy', 'Huyện', 82),
(818, 'Huyện Tân Phước', 'Huyện', 82),
(819, 'Huyện Cái Bè', 'Huyện', 82),
(820, 'Huyện Cai Lậy', 'Thị xã', 82),
(821, 'Huyện Châu Thành', 'Huyện', 82),
(822, 'Huyện Chợ Gạo', 'Huyện', 82),
(823, 'Huyện Gò Công Tây', 'Huyện', 82),
(824, 'Huyện Gò Công Đông', 'Huyện', 82),
(825, 'Huyện Tân Phú Đông', 'Huyện', 82),
(829, 'Thành phố Bến Tre', 'Thành phố', 83),
(831, 'Huyện Châu Thành', 'Huyện', 83),
(832, 'Huyện Chợ Lách', 'Huyện', 83),
(833, 'Huyện Mỏ Cày Nam', 'Huyện', 83),
(834, 'Huyện Giồng Trôm', 'Huyện', 83),
(835, 'Huyện Bình Đại', 'Huyện', 83),
(836, 'Huyện Ba Tri', 'Huyện', 83),
(837, 'Huyện Thạnh Phú', 'Huyện', 83),
(838, 'Huyện Mỏ Cày Bắc', 'Huyện', 83),
(842, 'Thành phố Trà Vinh', 'Thành phố', 84),
(844, 'Huyện Càng Long', 'Huyện', 84),
(845, 'Huyện Cầu Kè', 'Huyện', 84),
(846, 'Huyện Tiểu Cần', 'Huyện', 84),
(847, 'Huyện Châu Thành', 'Huyện', 84),
(848, 'Huyện Cầu Ngang', 'Huyện', 84),
(849, 'Huyện Trà Cú', 'Huyện', 84),
(850, 'Huyện Duyên Hải', 'Huyện', 84),
(851, 'Thị xã Duyên Hải', 'Thị xã', 84),
(855, 'Thành phố Vĩnh Long', 'Thành phố', 86),
(857, 'Huyện Long Hồ', 'Huyện', 86),
(858, 'Huyện Mang Thít', 'Huyện', 86),
(859, 'Huyện  Vũng Liêm', 'Huyện', 86),
(860, 'Huyện Tam Bình', 'Huyện', 86),
(861, 'Thị xã Bình Minh', 'Thị xã', 86),
(862, 'Huyện Trà Ôn', 'Huyện', 86),
(863, 'Huyện Bình Tân', 'Huyện', 86),
(866, 'Thành phố Cao Lãnh', 'Thành phố', 87),
(867, 'Thành phố Sa Đéc', 'Thành phố', 87),
(868, 'Thị xã Hồng Ngự', 'Thị xã', 87),
(869, 'Huyện Tân Hồng', 'Huyện', 87),
(870, 'Huyện Hồng Ngự', 'Huyện', 87),
(871, 'Huyện Tam Nông', 'Huyện', 87),
(872, 'Huyện Tháp Mười', 'Huyện', 87),
(873, 'Huyện Cao Lãnh', 'Huyện', 87),
(874, 'Huyện Thanh Bình', 'Huyện', 87),
(875, 'Huyện Lấp Vò', 'Huyện', 87),
(876, 'Huyện Lai Vung', 'Huyện', 87),
(877, 'Huyện Châu Thành', 'Huyện', 87),
(883, 'Thành phố Long Xuyên', 'Thành phố', 89),
(884, 'Thành phố Châu Đốc', 'Thành phố', 89),
(886, 'Huyện An Phú', 'Huyện', 89),
(887, 'Thị xã Tân Châu', 'Thị xã', 89),
(888, 'Huyện Phú Tân', 'Huyện', 89),
(889, 'Huyện Châu Phú', 'Huyện', 89),
(890, 'Huyện Tịnh Biên', 'Huyện', 89),
(891, 'Huyện Tri Tôn', 'Huyện', 89),
(892, 'Huyện Châu Thành', 'Huyện', 89),
(893, 'Huyện Chợ Mới', 'Huyện', 89),
(894, 'Huyện Thoại Sơn', 'Huyện', 89),
(899, 'Thành phố Rạch Giá', 'Thành phố', 91),
(900, 'Thị xã Hà Tiên', 'Thị xã', 91),
(902, 'Huyện Kiên Lương', 'Huyện', 91),
(903, 'Huyện Hòn Đất', 'Huyện', 91),
(904, 'Huyện Tân Hiệp', 'Huyện', 91),
(905, 'Huyện Châu Thành', 'Huyện', 91),
(906, 'Huyện Giồng Riềng', 'Huyện', 91),
(907, 'Huyện Gò Quao', 'Huyện', 91),
(908, 'Huyện An Biên', 'Huyện', 91),
(909, 'Huyện An Minh', 'Huyện', 91),
(910, 'Huyện Vĩnh Thuận', 'Huyện', 91),
(911, 'Huyện Phú Quốc', 'Huyện', 91),
(912, 'Huyện Kiên Hải', 'Huyện', 91),
(913, 'Huyện U Minh Thượng', 'Huyện', 91),
(914, 'Huyện Giang Thành', 'Huyện', 91),
(916, 'Quận Ninh Kiều', 'Quận', 92),
(917, 'Quận Ô Môn', 'Quận', 92),
(918, 'Quận Bình Thuỷ', 'Quận', 92),
(919, 'Quận Cái Răng', 'Quận', 92),
(923, 'Quận Thốt Nốt', 'Quận', 92),
(924, 'Huyện Vĩnh Thạnh', 'Huyện', 92),
(925, 'Huyện Cờ Đỏ', 'Huyện', 92),
(926, 'Huyện Phong Điền', 'Huyện', 92),
(927, 'Huyện Thới Lai', 'Huyện', 92),
(930, 'Thành phố Vị Thanh', 'Thành phố', 93),
(931, 'Thị xã Ngã Bảy', 'Thị xã', 93),
(932, 'Huyện Châu Thành A', 'Huyện', 93),
(933, 'Huyện Châu Thành', 'Huyện', 93),
(934, 'Huyện Phụng Hiệp', 'Huyện', 93),
(935, 'Huyện Vị Thuỷ', 'Huyện', 93),
(936, 'Huyện Long Mỹ', 'Huyện', 93),
(937, 'Thị xã Long Mỹ', 'Thị xã', 93),
(941, 'Thành phố Sóc Trăng', 'Thành phố', 94),
(942, 'Huyện Châu Thành', 'Huyện', 94),
(943, 'Huyện Kế Sách', 'Huyện', 94),
(944, 'Huyện Mỹ Tú', 'Huyện', 94),
(945, 'Huyện Cù Lao Dung', 'Huyện', 94),
(946, 'Huyện Long Phú', 'Huyện', 94),
(947, 'Huyện Mỹ Xuyên', 'Huyện', 94),
(948, 'Thị xã Ngã Năm', 'Thị xã', 94),
(949, 'Huyện Thạnh Trị', 'Huyện', 94),
(950, 'Thị xã Vĩnh Châu', 'Thị xã', 94),
(951, 'Huyện Trần Đề', 'Huyện', 94),
(954, 'Thành phố Bạc Liêu', 'Thành phố', 95),
(956, 'Huyện Hồng Dân', 'Huyện', 95),
(957, 'Huyện Phước Long', 'Huyện', 95),
(958, 'Huyện Vĩnh Lợi', 'Huyện', 95),
(959, 'Thị xã Giá Rai', 'Thị xã', 95),
(960, 'Huyện Đông Hải', 'Huyện', 95),
(961, 'Huyện Hoà Bình', 'Huyện', 95),
(964, 'Thành phố Cà Mau', 'Thành phố', 96),
(966, 'Huyện U Minh', 'Huyện', 96),
(967, 'Huyện Thới Bình', 'Huyện', 96),
(968, 'Huyện Trần Văn Thời', 'Huyện', 96),
(969, 'Huyện Cái Nước', 'Huyện', 96),
(970, 'Huyện Đầm Dơi', 'Huyện', 96),
(971, 'Huyện Năm Căn', 'Huyện', 96),
(972, 'Huyện Phú Tân', 'Huyện', 96),
(973, 'Huyện Ngọc Hiển', 'Huyện', 96);

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_order`
--

CREATE TABLE `db_order` (
  `id` int(11) NOT NULL,
  `orderCode` varchar(8) CHARACTER SET utf8 NOT NULL,
  `customerid` int(11) NOT NULL,
  `orderdate` datetime NOT NULL,
  `fullname` varchar(100) CHARACTER SET utf8 NOT NULL,
  `phone` varchar(100) CHARACTER SET utf8 NOT NULL,
  `money` int(12) NOT NULL,
  `price_ship` int(11) NOT NULL,
  `coupon` int(11) NOT NULL,
  `province` int(5) NOT NULL,
  `district` int(5) NOT NULL,
  `address` varchar(255) CHARACTER SET utf8 NOT NULL,
  `trash` int(1) NOT NULL DEFAULT 1,
  `status` int(1) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_order`
--

INSERT INTO `db_order` (`id`, `orderCode`, `customerid`, `orderdate`, `fullname`, `phone`, `money`, `price_ship`, `coupon`, `province`, `district`, `address`, `trash`, `status`) VALUES
(77, 'ZpCUeDP4', 68, '2022-05-22 01:31:49', 'Vũ Hồng', '0358782665', 820000, 30000, 0, 31, 318, 'zczhbkn ', 1, 1),
(78, '6W4AzcVl', 69, '2022-05-22 01:35:52', 'HonaGF', '0746363533', 620000, 30000, 0, 4, 46, 'gbthehe', 1, 1),
(79, 'VJGcmZbP', 69, '2022-05-22 20:01:28', 'HonaGF', '0746363533', 240000, 30000, 0, 79, 768, 'phus nhuan', 1, 1),
(80, '5FQdhwvU', 69, '2022-05-22 20:02:00', 'HonaGF', '0746363533', 478000, 30000, 0, 89, 893, 'an giang', 1, 2),
(81, 'szcNHltj', 69, '2022-05-22 20:02:43', 'HonaGF', '0746363533', 354000, 30000, 0, 48, 495, 'cam thuy', 1, 2),
(82, 'sIk8XA6T', 70, '2022-05-22 20:03:24', 'Vũ Thị hợp', '0378706959', 280000, 30000, 0, 1, 274, 'thanhoia', 1, 1),
(83, 'nYgdSVZ2', 69, '2022-05-22 20:05:09', 'HonaGF', '0746363533', 516000, 30000, 0, 1, 1, 'phuong 1', 1, 2),
(84, 'aHcy1gWl', 69, '2022-05-22 20:06:12', 'HonaGF', '0746363533', 425000, 30000, 0, 31, 318, 'thuy anh', 1, 2),
(85, 'fGAOm0un', 69, '2022-05-22 20:06:58', 'HonaGF', '0746363533', 48480, 30000, 0, 31, 317, 'ddhd', 1, 1),
(86, 'ERjDmXZx', 69, '2022-05-23 23:26:07', 'HonaGF', '0746363533', 6530000, 30000, 0, 31, 318, 'dgs', 1, 0),
(87, 'b42HugyI', 69, '2022-06-05 16:25:09', 'HonaGF', '0746363533', 230000, 30000, 0, 31, 313, 'kiến an', 1, 2),
(88, 'aMP4QtqZ', 69, '2022-06-06 01:15:48', 'HonaGF', '0746363533', 804000, 30000, 0, 48, 495, 'an lão', 1, 0),
(89, 'nB3eg8L7', 72, '2022-06-06 01:26:03', 'Lê Thị Nga', '0368259264', 428000, 30000, 0, 31, 317, 'an lão', 1, 2),
(90, 'tg1cPmHl', 73, '2022-06-08 01:11:46', 'ffsfsfdsf', '12313123', 516000, 30000, 0, 31, 314, 'saedasdadasdas', 1, 0),
(91, '10WcnelJ', 69, '2022-06-11 18:56:24', 'HonaGF', '0746363533', 430000, 30000, 0, 92, 925, 'rfrff', 1, 0),
(92, 'ceS2z8lE', 69, '2022-06-11 19:29:31', 'HonaGF', '0746363533', 280000, 30000, 0, 1, 277, 'đê', 1, 0),
(93, 'HAbnMCaR', 69, '2022-06-11 19:32:18', 'HonaGF', '0746363533', 170000, 30000, 0, 1, 18, 'cdcdcdc', 1, 0),
(94, 'dqw5jlLb', 69, '2022-06-11 19:38:12', 'HonaGF', '0746363533', 150000, 30000, 0, 1, 18, 'ffđf', 1, 0),
(95, 'mnhrYJCb', 69, '2022-06-11 19:43:21', 'HonaGF', '0746363533', 160000, 30000, 0, 1, 271, 'fr', 1, 0),
(96, 'p3conJ1x', 69, '2022-06-11 19:44:09', 'HonaGF', '0746363533', 72500, 30000, 0, 1, 274, 'd', 1, 0),
(97, 't2isaNz6', 69, '2022-06-11 19:45:03', 'HonaGF', '0746363533', 160000, 30000, 0, 1, 18, 'scs', 1, 0),
(98, '6YC5PEUw', 69, '2022-06-11 19:45:58', 'HonaGF', '0746363533', 160000, 30000, 0, 79, 783, 'de', 1, 0),
(99, 'DQk18Cou', 69, '2022-06-11 19:47:10', 'HonaGF', '0746363533', 160000, 30000, 0, 1, 271, 'dedde', 1, 0),
(100, '2AvE5N0b', 69, '2022-06-11 19:48:45', 'HonaGF', '0746363533', 225000, 30000, 0, 1, 271, 'd', 1, 0),
(101, '3TG0mLay', 69, '2022-06-11 19:54:07', 'HonaGF', '0746363533', 160000, 30000, 0, 1, 271, 'đe', 1, 2),
(102, 'e90lOK7Z', 69, '2022-06-11 19:54:21', 'HonaGF', '0746363533', 160000, 30000, 0, 1, 271, 'đe', 1, 0),
(103, 'xX57aLz4', 69, '2022-06-11 19:55:52', 'HonaGF', '0746363533', 932160, 30000, 0, 48, 490, 'dsf', 1, 0),
(104, 'nKaFoksr', 69, '2022-06-11 19:59:11', 'HonaGF', '0746363533', 1324000, 30000, 0, 31, 313, 'hh', 1, 2),
(105, 'XUKQGwDC', 69, '2022-06-11 20:02:32', 'HonaGF', '0746363533', 2230000, 30000, 0, 31, 313, 'hhh', 1, 2),
(106, 'LU1r2TGH', 74, '2022-06-11 20:20:59', 'nga lê', '0987564432', 230000, 30000, 0, 92, 925, 'đ', 1, 0),
(107, 'HJAimkW0', 74, '2022-06-11 20:22:10', 'nga lê', '0987564432', 1374000, 30000, 0, 92, 927, 'jhfjf', 1, 0),
(108, 'swmaH04A', 74, '2022-06-11 20:32:57', 'nga lê', '0987564432', 370000, 30000, 150000, 48, 498, 'rrte', 1, 0);

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_orderdetail`
--

CREATE TABLE `db_orderdetail` (
  `id` int(11) NOT NULL,
  `orderid` int(11) NOT NULL,
  `productid` int(11) NOT NULL,
  `count` int(10) NOT NULL,
  `price` int(11) NOT NULL,
  `trash` tinyint(1) NOT NULL DEFAULT 1,
  `status` tinyint(1) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_orderdetail`
--

INSERT INTO `db_orderdetail` (`id`, `orderid`, `productid`, `count`, `price`, `trash`, `status`) VALUES
(102, 77, 14, 1, 790000, 1, 1),
(103, 78, 28, 1, 60000, 1, 1),
(104, 78, 15, 1, 200000, 1, 1),
(105, 78, 24, 1, 80000, 1, 1),
(106, 78, 22, 1, 250000, 1, 1),
(107, 79, 9, 1, 10000, 1, 1),
(108, 79, 15, 1, 200000, 1, 1),
(109, 80, 8, 1, 448000, 1, 1),
(110, 81, 13, 2, 162000, 1, 1),
(111, 82, 22, 1, 250000, 1, 1),
(112, 83, 13, 3, 162000, 1, 1),
(113, 84, 2, 1, 200000, 1, 1),
(114, 84, 23, 1, 195000, 1, 1),
(115, 85, 10, 3, 6160, 1, 1),
(116, 86, 31, 50, 130000, 1, 1),
(117, 87, 15, 1, 200000, 1, 1),
(118, 88, 7, 2, 387000, 1, 1),
(119, 89, 16, 1, 398000, 1, 1),
(122, 99, 31, 1, 130000, 1, 1),
(123, 100, 23, 1, 195000, 1, 1),
(124, 102, 31, 1, 130000, 1, 1),
(125, 103, 8, 2, 448000, 1, 1),
(126, 103, 10, 1, 6160, 1, 1),
(127, 104, 8, 2, 448000, 1, 1),
(128, 104, 16, 1, 398000, 1, 1),
(129, 105, 2, 11, 200000, 1, 1),
(130, 106, 15, 1, 200000, 1, 1),
(131, 107, 8, 3, 448000, 1, 1),
(132, 108, 12, 1, 490000, 1, 1);

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_producer`
--

CREATE TABLE `db_producer` (
  `id` int(10) NOT NULL,
  `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `code` varchar(100) COLLATE utf8_unicode_ci NOT NULL,
  `keyword` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `created_at` datetime NOT NULL,
  `created_by` int(11) NOT NULL,
  `modified` datetime NOT NULL,
  `modified_by` int(11) NOT NULL,
  `status` int(11) NOT NULL DEFAULT 1,
  `trash` int(11) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_producer`
--

INSERT INTO `db_producer` (`id`, `name`, `code`, `keyword`, `created_at`, `created_by`, `modified`, `modified_by`, `status`, `trash`) VALUES
(1, 'Digiworld', 'DIGIWORLD', 'mipham,toner,kembb', '2019-05-22 16:08:31', 4, '2022-05-20 01:19:10', 1, 1, 1),
(2, 'Nhà cung cấp di động gò vấp', 'GOVAPPRODUCER', ' dienthoai, maytinhbang,laptop,phukien', '2019-06-12 23:30:37', 1, '2019-07-05 10:52:13', 1, 1, 0),
(3, 'Loreal', 'TGDDCODE', 'toner,chongnang, son, phanmat', '2019-05-22 16:06:31', 4, '2022-05-20 16:04:03', 1, 1, 1),
(4, 'Vichy', 'vi3j1h3', 'chongnang, suaruamat', '2022-05-19 23:35:55', 1, '2022-05-19 23:35:55', 1, 1, 1),
(5, 'COCO', 'coco', 'chongnang, suaruamat, mipham,lamdep', '2022-05-20 01:18:33', 1, '2022-05-20 01:18:33', 1, 1, 1),
(6, 'Samishop', 'samishop', 'chongnang, suaruamat, mipham,lamdep, phanmat, sonmoi', '2022-05-20 16:00:38', 1, '2022-05-20 16:00:38', 1, 1, 1);

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_product`
--

CREATE TABLE `db_product` (
  `id` int(11) NOT NULL,
  `catid` int(11) NOT NULL,
  `name` varchar(255) CHARACTER SET utf8 NOT NULL,
  `alias` varchar(255) CHARACTER SET utf8 NOT NULL,
  `avatar` varchar(255) CHARACTER SET utf8 NOT NULL,
  `img` varchar(255) CHARACTER SET utf8 NOT NULL,
  `sortDesc` text CHARACTER SET utf8 NOT NULL,
  `detail` text CHARACTER SET utf8 NOT NULL,
  `producer` int(11) NOT NULL,
  `number` int(11) NOT NULL,
  `number_buy` int(11) NOT NULL,
  `sale` int(3) DEFAULT NULL,
  `price` int(11) NOT NULL,
  `price_sale` int(11) NOT NULL,
  `created` datetime NOT NULL,
  `created_by` varchar(100) CHARACTER SET utf8 NOT NULL DEFAULT 'HDL',
  `modified` datetime NOT NULL,
  `modified_by` varchar(100) CHARACTER SET utf8 NOT NULL DEFAULT 'HDL',
  `trash` int(1) NOT NULL DEFAULT 1,
  `status` int(1) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_product`
--

INSERT INTO `db_product` (`id`, `catid`, `name`, `alias`, `avatar`, `img`, `sortDesc`, `detail`, `producer`, `number`, `number_buy`, `sale`, `price`, `price_sale`, `created`, `created_by`, `modified`, `modified_by`, `trash`, `status`) VALUES
(2, 1, 'Thuốc nhuộm tóc Nâu Tây không tẩy sử dụng tại nhà nhiều thảo dược giá rẻ kcolor', 'thuoc-nhuom-toc-nau-tay-khong-tay-su-dung-tai-nha-nhieu-thao-duoc-gia-re-kcolor', 'tnhuom.jpg', 'tnhuom.jpg', '', '<p>B&agrave;i viết đang được cập nhật</p>\r\n', 1, 32, 31, 10, 220000, 200000, '2022-05-20 14:06:56', '1', '2022-06-10 00:29:28', '1', 1, 1),
(4, 2, 'Thuốc nhuộm tóc Xanh đen khói', 'thuoc-nhuom-toc-xanh-den-khoi', 'tnhuom3.jpg', 'tnhuom3.jpg', '', 'Bài viết cho sản phẩm này đang được cập nhật ..', 5, 400, 44, 10, 49900, 42500, '2022-05-20 00:00:00', '1', '2022-05-20 00:00:00', '1', 1, 1),
(5, 2, 'Thuốc nhuộm tóc chiết xuất thảo dược, hàng nhập khẩu, màu Vàng ánh khói', 'Thuốc nhuộm tóc chiết xuất thảo dược, hàng nhập khẩu, màu Vàng ánh khói', 'tnhuom4.jpg', 'tnhuom4.jpg', '', 'Bài viết đang được cập nhật', 5, 368, 11, 10, 69900, 63900, '2022-05-20 00:00:00', '1', '2022-05-20 00:00:00', '1', 1, 1),
(6, 8, 'Tinh Chất Tái Tạo Phục Hồi Da Săn Chắc Sur.Medic Super Peptide Collagen Lifting Serum 50ml', 'Tinh Chất Tái Tạo Phục Hồi Da Săn Chắc Sur.Medic Super Peptide Collagen Lifting Serum 50ml', 'serum1.jpg', 'serum1.jpg', '', 'Bài viết đang được cập nhật\r\n', 4, 420, 10, 10, 499000, 455000, '2022-05-20 14:18:04', '1', '2022-05-20 14:18:04', '1', 1, 1),
(7, 1, 'Toner nước bí đao', 'toner-nuoc-bi-dao', 'bidao1.jpg', 'bidao1.jpg', 'Toner nước bí đao cân bằng da cocoon 310ml thuần chay', '', 5, 190, 21, 10, 430000, 387000, '2019-06-03 22:04:43', '1', '2022-05-20 15:53:55', '1', 1, 1),
(8, 1, 'Bộ sản phẩm chăm sóc da,ngăn ngừa mụn DAILY EFFECT', 'bo-san-pham-cham-da-ngua-mun-daily-effect', 'chamda3.jpg', 'chamda3.jpg', 'Bộ sản phẩm chăm sóc da,ngăn ngừa mụn DAILY EFFECT', 'Bài viết đang được cập nhật', 6, 160, 33, 20, 560000, 448000, '2022-05-20 00:00:00', '1', '2022-05-20 00:00:00', '1', 1, 1),
(9, 7, 'Mặt nạ dưỡng da, mặt nạ dưỡng trắng', 'mat-na-duong-da-mat-na-duong-trang', 'matna41.jpg', 'matna41.jpg', 'Mặt nạ dưỡng da, mặt nạ dưỡng trắng cấp ẩm mờ thâm bổ sung collagen tinh chất ốc sên Nội Địa Trung', '<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">I. Th&ocirc;ng tin sản phẩm: Mặt Nạ Ốc S&ecirc;n Collagen</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- NET: 25g</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- HSD: 3 năm kể từ ng&agrave;y sản xuất (h&agrave;ng nhập mới mỗi th&aacute;ng)</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- Ng&agrave;y sản xuất: Xem tr&ecirc;n bao b&igrave;</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- Xuất xứ: H&agrave;ng nội địa trung </span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">II. C&ocirc;ng Dụng Nổi Bật:</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- Với tinh chất chất dịch nhầy ốc s&ecirc;n gi&agrave;u dưỡng chất arbutin v&agrave; collagen thẳm thấu v&agrave;o cơ mặt, bổ sung độ ẩm, t&aacute;i tạo l&agrave;n da bị sạm, mờ nếp nhăn, trẻ ho&aacute;, gi&uacute;p da săn chắc, mịn m&agrave;ng. </span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">Da đều m&agrave;u, trắng s&aacute;ng. * Mặt nạ ốc s&ecirc;n Dưỡng Ẩm (moisturizing):</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- Chiết suất dịch nhầy ốc s&ecirc;n kết hợp tinh chất rong biển.</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- Dưỡng ẩm s&acirc;u cho da kh&ocirc;.</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- L&agrave;m dịu, mềm mịn da.</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- Bổ sung dưỡng chất gi&uacute;p t&aacute;i tạo da, l&agrave;m mờ nếp nhăn, ngăn l&atilde;o h&oacute;a.</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- Rong biển gi&agrave;u vitamin v&agrave; kho&aacute;ng chất gi&uacute;p, hạn chế tiết dầu, l&agrave;m dịu da. </span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">* Mặt nạ ốc s&ecirc;n Cấp Nước (hydrating):</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- Chiết suất dịch nhầy ốc s&ecirc;n kết hợp tinh chất nha đam v&agrave; rong biển.</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- Cấp nước cho da kh&ocirc;, c&acirc;n bằng sắc tố.</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- Bổ sung dưỡng chất gi&uacute;p da căng mọng nước, mờ nếp nhăn, s&aacute;ng da.</span></p>\r\n<p>\r\n	<span style=\"color: rgba(0, 0, 0, 0.8); font-family: &quot;Helvetica Neue&quot;, Helvetica, Arial, 文泉驛正黑, &quot;WenQuanYi Zen Hei&quot;, &quot;Hiragino Sans GB&quot;, &quot;儷黑 Pro&quot;, &quot;LiHei Pro&quot;, &quot;Heiti TC&quot;, 微軟正黑體, &quot;Microsoft JhengHei UI&quot;, &quot;Microsoft JhengHei&quot;, sans-serif; font-size: 14px; white-space: pre-wrap;\">- L&agrave;m dịu da.</span></p>\r\n', 2, 220, 30, 10, 20000, 10000, '2022-05-20 00:00:00', '1', '2022-05-20 00:00:00', '1', 1, 1),
(10, 7, 'Mặt Nạ Sữa Tươi Trái Cây Beotua', 'mat-na-sua-tuoi-trai-cay-beotua', 'matna6.jpg', 'matna7.jpg', 'Mặt Nạ Sữa Tươi Trái Cây Beotua- Nguồn Dinh Dưỡng Cho Da', '<p>\r\n	<span font-size:=\"\" heiti=\"\" helvetica=\"\" hiragino=\"\" jhenghei=\"\" lihei=\"\" microsoft=\"\" sans=\"\" style=\"color: rgba(0, 0, 0, 0.8); font-family: \" wenquanyi=\"\" white-space:=\"\" zen=\"\">Mặt nạ sữa tươi tr&aacute;i c&acirc;y</span><span font-size:=\"\" heiti=\"\" helvetica=\"\" hiragino=\"\" jhenghei=\"\" lihei=\"\" microsoft=\"\" sans=\"\" style=\"color: rgba(0, 0, 0, 0.8); font-family: \" wenquanyi=\"\" white-space:=\"\" zen=\"\"> - Nguồn dinh dưỡng tuyệt vời cho l&agrave;n da- Ch&iacute;nh h&atilde;ng Beotua </span></p>\r\n<p>\r\n	<span font-size:=\"\" heiti=\"\" helvetica=\"\" hiragino=\"\" jhenghei=\"\" lihei=\"\" microsoft=\"\" sans=\"\" style=\"color: rgba(0, 0, 0, 0.8); font-family: \" wenquanyi=\"\" white-space:=\"\" zen=\"\">C&ocirc;ng dụng: </span></p>\r\n<p>\r\n	<span font-size:=\"\" heiti=\"\" helvetica=\"\" hiragino=\"\" jhenghei=\"\" lihei=\"\" microsoft=\"\" sans=\"\" style=\"color: rgba(0, 0, 0, 0.8); font-family: \" wenquanyi=\"\" white-space:=\"\" zen=\"\">&nbsp;Sữa tươi v&agrave; tr&aacute;i D&acirc;u: Cấp ẩm dưỡng trắng da mịn m&agrave;ng</span></p>\r\n<p>\r\n	<span font-size:=\"\" heiti=\"\" helvetica=\"\" hiragino=\"\" jhenghei=\"\" lihei=\"\" microsoft=\"\" sans=\"\" style=\"color: rgba(0, 0, 0, 0.8); font-family: \" wenquanyi=\"\" white-space:=\"\" zen=\"\">&nbsp;Chiết xuất từ sữa tươi v&agrave; tr&aacute;i việt quất gi&uacute;p l&agrave;m trắng da, s&aacute;ng khoẻ v&agrave; trắng hồng mịn m&agrave;ng. Gi&uacute;p da bớt nhờn v&agrave; mụn. L&agrave;m giảm qu&aacute; tr&igrave;nh l&atilde;o ho&aacute;. </span></p>\r\n<p>\r\n	<span font-size:=\"\" heiti=\"\" helvetica=\"\" hiragino=\"\" jhenghei=\"\" lihei=\"\" microsoft=\"\" sans=\"\" style=\"color: rgba(0, 0, 0, 0.8); font-family: \" wenquanyi=\"\" white-space:=\"\" zen=\"\">&nbsp;Chiết xuất từ sữa tươi v&agrave; tr&aacute;i bơ, giữ ẩm, giảm kh&ocirc; da, bong tr&oacute;c. L&agrave;m mờ dần c&aacute;c vết th&acirc;m, vết sẹo do mụn. </span></p>\r\n<p>\r\n	<span font-size:=\"\" heiti=\"\" helvetica=\"\" hiragino=\"\" jhenghei=\"\" lihei=\"\" microsoft=\"\" sans=\"\" style=\"color: rgba(0, 0, 0, 0.8); font-family: \" wenquanyi=\"\" white-space:=\"\" zen=\"\">&nbsp;Điểm đặc biệt của c&aacute;c em mặt nạ Beotua l&agrave; miếng mask rất mỏng, khi đắp rất &ocirc;m mặt c&aacute;c bạn nha </span></p>\r\n<p>\r\n	<span font-size:=\"\" heiti=\"\" helvetica=\"\" hiragino=\"\" jhenghei=\"\" lihei=\"\" microsoft=\"\" sans=\"\" style=\"color: rgba(0, 0, 0, 0.8); font-family: \" wenquanyi=\"\" white-space:=\"\" zen=\"\">&nbsp;V&agrave; hương thơm của từng vị cực tự nhi&ecirc;n thơm m&aacute;t, chứ ko như m&ugrave;i h&oacute;a học như c&aacute;c loại kh&aacute;c đ&acirc;u nha Ch&iacute;nh 2 điểm cộng n&agrave;y, khiến mặt nạ Beotua nổi l&ecirc;n như cồn trong thời gian qua, shop cực ưng khi trải nghiệm từng mask, lạc v&agrave;o rừng hoa rừng tr&aacute;i c&acirc;y ngất ng&acirc;y lắm đ&oacute;a </span></p>\r\n<p>\r\n	<span font-size:=\"\" heiti=\"\" helvetica=\"\" hiragino=\"\" jhenghei=\"\" lihei=\"\" microsoft=\"\" sans=\"\" style=\"color: rgba(0, 0, 0, 0.8); font-family: \" wenquanyi=\"\" white-space:=\"\" zen=\"\">⛔️ C&aacute;ch sử dụng: Rửa mặt sạch, đắp mask l&ecirc;n da. Sau 15-20p, gỡ bỏ lớp mặt nạ rồi massage đều để da hấp thụ dưỡng chất. Sau đ&oacute; thoa toner v&agrave; c&aacute;c bước skincare sau. </span></p>\r\n<p>\r\n	<span font-size:=\"\" heiti=\"\" helvetica=\"\" hiragino=\"\" jhenghei=\"\" lihei=\"\" microsoft=\"\" sans=\"\" style=\"color: rgba(0, 0, 0, 0.8); font-family: \" wenquanyi=\"\" white-space:=\"\" zen=\"\">⚠️ N&Ecirc;N SỬ DỤNG 2-3 LẦN /1 TUẦN. v&agrave;o buổi tối nha</span></p>\r\n', 6, 310, 43, 12, 7000, 6160, '2022-05-20 00:00:00', '1', '2022-05-20 16:48:33', '1', 1, 1),
(11, 9, 'Son môi Herorange', 'son-moi-herorange', 'sonkem2.jpg', 'sonkem3.jpg', 'Son môi Herorange màu nhung lì mềm mại chống thấm nước lâu trôi nội địa trung', 'Bài viết đang đang được cập nhật', 6, 605, 27, 10, 340000, 306000, '2022-05-20 00:00:00', '1', '2022-05-20 00:00:00', '1', 1, 1),
(12, 8, 'Tinh Chất Angel\'s Liquid Dưỡng Sáng Da, Mờ Thâm', 'tinh-chat-angels-liquid-duong-sang-da-mo-tham', 'tinhchat.jpg', 'tinhchat.jpg', 'Tinh Chất Angel\'s Liquid Dưỡng Sáng Da, Mờ Thâm 7 Day Whitening Program Glutathione 700 V Ampoule 30ml', '<p><em>Loại da ph&ugrave; hợp:</em></p>\r\n\r\n<p><em>Sản phẩm ph&ugrave; hợp cho mọi loại da.</em></p>\r\n\r\n<p><em>C&ocirc;ng dụng:</em></p>\r\n\r\n<p><em>Gi&uacute;p cải thiện t&ocirc;ng m&agrave;u da s&aacute;ng hồng rạng rỡ.</em></p>\r\n\r\n<p><em>Gi&uacute;p l&agrave;m mờ th&acirc;m n&aacute;m, đồi mồi, t&agrave;n nhang v&agrave; c&aacute;c khuyết điểm da kh&ocirc;ng đều m&agrave;u.</em></p>\r\n\r\n<p><em>Cung cấp dưỡng chất, chất chống oxy h&oacute;a cao nu&ocirc;i dưỡng v&agrave; bảo vệ tế b&agrave;o da một c&aacute;ch triệt để.</em></p>\r\n\r\n<p><em>Hỗ trợ cải thiện qu&aacute; tr&igrave;nh l&atilde;o h&oacute;a da, phục hồi l&agrave;n da bị sần s&ugrave;i, kh&ocirc; r&aacute;p, mất nước.</em></p>\r\n\r\n<p><em>Dưỡng ẩm da mềm mại, mịn m&agrave;ng.</em></p>\r\n\r\n<p><em>Gi&uacute;p thu nhỏ lỗ ch&acirc;n l&ocirc;ng, hạn chế t&igrave;nh trạng mụn trứng c&aacute;.</em></p>\r\n\r\n<p><em>Bảo vệ c&aacute;c dưỡng chất dưới da, tăng sản sinh collagen gi&uacute;p da đ&agrave;n hồi, săn chắc.</em></p>\r\n\r\n<p><em>Bảo quản:</em></p>\r\n\r\n<p><em>Bảo quản nơi kh&ocirc; r&aacute;o, tho&aacute;ng m&aacute;t.</em></p>\r\n\r\n<p><em>Tr&aacute;nh &aacute;nh nắng trực tiếp mặt trời.</em></p>\r\n\r\n<p><em>Đậy k&iacute;n nắp sau khi sử dụng.</em></p>\r\n', 1, 220, 4, 0, 490000, 490000, '2019-06-03 22:27:35', '1', '2022-05-20 17:08:43', '1', 1, 1),
(13, 8, 'Serum Cấp Nước Dưỡng Ẩm Sâu Da Mặt Balance', 'serum-cap-nuoc-duong-am-sau-da-mat-balance', 'serum2.jpg', 'serum2.jpg', 'Serum Cấp Nước Dưỡng Ẩm Sâu Da Mặt Balance Active Formula /Hyaluronic Deep Moisturizing 30ml', '<p><em>Serum Cấp Nước Dưỡng Ẩm S&acirc;u Da Mặt Balance Active Formula /Hyaluronic Deep Moisturizing 30ml</em></p>\r\n\r\n<p><em>???? TH&Ocirc;NG TIN SẢN PHẨM:<br />\r\n- Loại sản phẩm: Sẻum Balance Active Formula /Hyaluronic Deep Moisturizing<br />\r\n- Xuất xứ: Anh<br />\r\n- Quy c&aacute;ch: 30ml/lọ<br />\r\n- BẢO QUẢN: Nơi tho&aacute;ng m&aacute;t, tr&aacute;nh &aacute;nh nắng trực tiếp, tr&aacute;nh va đập mạnh.<br />\r\n- Ng&agrave;y sản xuất: Xem tr&ecirc;n bao b&igrave;<br />\r\n- Thời hạn sử dụng: 3 năm</em></p>\r\n\r\n<p><em>???? ĐẶC ĐIỂM NỔI BẬT:<br />\r\n- Balance Active Formula l&agrave; thương hiệu mỹ phẩm đến từ Anh với phương ch&acirc;m tạo ra c&aacute;c sản phẩm chăm s&oacute;c da c&oacute; th&agrave;nh phần độc đ&aacute;o, hiệu quả cao với mức gi&aacute; b&igrave;nh d&acirc;n.<br />\r\n- Serum cấp nước dưỡng ẩm s&acirc;u cho da mặt Balance Active Formula Hyaluronic deep moisturing với th&agrave;nh phần ch&iacute;nh l&agrave; axit hyaluronic gi&uacute;p tăng cường hiệu quả dưỡng ẩm cho da nhờ khả năng h&uacute;t ẩm v&agrave; giữ nước vượt trội.<br />\r\n- LƯU &Yacute;: VỎ HỘP SẢN PHẨM ẢNH NẮP R&Atilde;NH, TUY NHI&Ecirc;N NẮP B&Ecirc;N TRONG CỦA SẢN PHẨM HIỆN TẠI Đ&Atilde; L&Agrave; NẮP TRƠN, H&Atilde;NG THAY ĐỔI VỎ NẮP KH&Ocirc;NG C&Ograve;N R&Atilde;NH NHƯ MẪU CŨ</em></p>\r\n\r\n<p><em>???? C&Ocirc;NG DỤNG:<br />\r\n- Dưỡng ẩm cho da, phục hồi t&igrave;nh trạng kh&ocirc; da ngay sau khi d&ugrave;ng<br />\r\n- Cải thiện độ đ&agrave;n hồi của da, da mềm mịn v&agrave; căng b&oacute;ng hơn<br />\r\n- Giảm c&aacute;c dấu hiệu l&atilde;o h&oacute;a, vết nhăn, vết ch&acirc;n chim<br />\r\n- Giảm tiết dầu nhờn, hạn chế mụn<br />\r\n- Hỗ trợ tăng c&aacute;c sản phẩm kh&aacute;c tăng cường hiệu quả. V&igrave; khi da đủ độ ẩm sẽ dễ d&agrave;ng hấp thụ c&aacute;c dưỡng chất kh&aacute;c.</em></p>\r\n\r\n<p><em>???? TH&Agrave;NH PHẦN:<br />\r\n- 5% Hyasol PF: một dạng thức của axit hyaluronic gi&uacute;p tăng cường khả năng h&uacute;t nước, giữ nước, dưỡng ẩm.</em></p>\r\n\r\n<p><em>???? HƯỚNG DẪN SỬ DỤNG:<br />\r\n- Sau khi rửa mặt sạch v&agrave; c&acirc;n bằng độ ẩm cho da bằng nước hoa hồng, cho 3-4 giọt serum ra tay v&agrave; xoa đều.&nbsp;<br />\r\n- Apply l&ecirc;n mặt bằng động t&aacute;c vỗ nhẹ từ 1-2 ph&uacute;t cho serum thẩm thấu v&agrave;o da.&nbsp;<br />\r\n- D&ugrave;ng serum h&agrave;ng ng&agrave;y cả s&aacute;ng v&agrave; tối để mang lại hiệu quả nhanh hơn.&nbsp;</em></p>\r\n\r\n<p><em>???? KIỂM NGHIỆM AN TO&Agrave;N:<br />\r\n- D&ugrave;ng được cho phụ nữ mang thai v&agrave; mẹ cho con b&uacute;<br />\r\n- Kh&ocirc;ng chất bảo quản (paraben) v&agrave; m&agrave;u h&oacute;a học<br />\r\n- Đ&atilde; được nghi&ecirc;n cứu v&agrave; kiểm nghiệm tại viện da liễu Anh Quốc.<br />\r\n__________________________________________________________<br />\r\n???? &nbsp;Kh&aacute;ch h&agrave;ng cần lấy ho&aacute; đơn đỏ vui l&ograve;ng li&ecirc;n hệ chat với SUN_WAREHOUSE để được hỗ trợ</em></p>\r\n\r\n<p><em>#sacdep #chamsocdamat #tinhchatduong #BalanceActiveFormula #sun_warehouse #tinhchatcapam #tinhchatduongda #serumduongam #serumduongdachuyensau #chamsocdamat #duongamchuyensau #serumcaocap #chonglaohoa #shopee</em><br />\r\n&nbsp;</p>\r\n', 6, 260, 21, 40, 270000, 162000, '2019-06-03 22:30:05', '1', '2022-05-20 17:06:59', '1', 1, 1),
(14, 6, 'Kem Chống Nắng La Roche Posay', 'kem-chong-nang-la-roche-posay', 'larochay.jpg', 'larochay.jpg', 'Kem Chống Nắng La Roche Posay Anthelios Anti - Shine Dry Touch Gel Cream SPF 50+ Cho Da Dầu 50ml', '<p><em>Kem Chống Nắng La Roche Posay Anthelios Anti - Shine Dry Touch Gel Cream SPF 50+ Cho Da Dầu 50ml</em></p>\r\n\r\n<p><em>✔️ Loại da:<br />\r\n&nbsp;- Ph&ugrave; hợp cho da dầu &amp;amp; da nhờn<br />\r\n&nbsp;- Da nhạy cảm dễ k&iacute;ch ứng với &aacute;nh nắng. Kh&ocirc;ng g&acirc;y nh&acirc;n mụn.&nbsp;<br />\r\n&nbsp;- Hoạt chất AIRLICIUM: th&agrave;nh phần gi&uacute;p giảm dầu, mồ h&ocirc;i &amp;amp; b&atilde; nhờn dư thừa<br />\r\n&nbsp;- Hiệu quả vượt trội, c&oacute; thể hấp thu lượng b&atilde; nhờn gấp 100 lần khối lượng<br />\r\n&nbsp;- Kiểm so&aacute;t b&atilde; nhờn &amp;amp; mồ h&ocirc;i gi&uacute;p mang đến một cảm gi&aacute;c &ldquo;sạch&rdquo; cho l&agrave;n da đến 9h</em></p>\r\n\r\n<p><em>✔️ Th&agrave;nh phần v&agrave; c&ocirc;ng dụng:<br />\r\nC&ocirc;ng nghệ độc quyền XL-PROTECT chống nắng tối ưu, bảo vệ da trước tia UVA-UVB, tia hồng ngoại &amp;amp; t&aacute;c hại từ &ocirc; nhiễm, kết hợp với nước kho&aacute;ng La Roche-Posay gi&uacute;p l&agrave;m dịu v&agrave; chống oxy ho&aacute;.<br />\r\n&nbsp;- Gồm nhiều m&agrave;ng lọc tia UV c&oacute; quang phổ rộng gi&uacute;p bảo vệ da trước ngay cả tia UVA d&agrave;i<br />\r\n&nbsp;- Bảo vệ da trước những t&aacute;c hại từ &aacute;nh nắng &amp;amp; &ocirc; nhiễm: l&atilde;o h&oacute;a sớm, đốm n&acirc;u, n&aacute;m, k&iacute;ch ứng &aacute;nh nắng<br />\r\n&nbsp;- Kết cấu dạng kem gel, thẩm thấu tức th&igrave;, mang lại cảm gi&aacute;c kh&ocirc; tho&aacute;ng, kh&ocirc;ng để lại vệt trắng<br />\r\n&nbsp;<br />\r\n✔️ C&Aacute;CH SỬ DỤNG:<br />\r\n&nbsp;- Thoa kem trước khi tiếp x&uacute;c với &aacute;nh nắng 20 ph&uacute;t.<br />\r\n&nbsp;- Lấy một lượng sản phẩm vừa đủ v&agrave; chấm 5 điểm tr&ecirc;n mặt (tr&aacute;n, mũi, cằm v&agrave; hai b&ecirc;n m&aacute;) sau đ&oacute; thoa sản phẩm theo chiều từ trong ra ngo&agrave;i v&agrave; tr&ecirc;n xuống dưới.<br />\r\n&nbsp;- Da được bảo vệ tối ưu khỏi tia UVA, UVB, tia hồng ngoại &amp;amp; t&aacute;c hại từ &ocirc; nhiễm.<br />\r\n&nbsp;- Kh&ocirc; tho&aacute;ng tức th&igrave; nhưng kh&ocirc;ng l&agrave;m qu&aacute; kh&ocirc; bề mặt da.<br />\r\n&nbsp;- Kh&ocirc;ng để lại vệt trắng, kh&ocirc;ng g&acirc;y b&oacute;ng nhờn.</em></p>\r\n\r\n<p><em>⚜️ Lưu &yacute;:&nbsp;<br />\r\n- Sản phẩm được m&igrave;nh đặt mua trực tiếp tại Pharmacie Ph&aacute;p, sản phẩm được ph&acirc;n phối tại Ph&aacute;p kh&ocirc;ng c&oacute; tem ni&ecirc;m phong, nilon hay seal. Shop cam kết h&agrave;ng ch&iacute;nh h&atilde;ng, mới 100%. Qu&yacute; kh&aacute;ch y&ecirc;n t&acirc;m về chất lượng sản phẩm</em></p>\r\n\r\n<p><em>???? Kem chống nắng La Roche Posay Anthelios Anti-Shine Dry Touch Gel Cream SPF 50+ khi b&ocirc;i l&ecirc;n da sẽ dễ bị v&oacute;n cục, tạo gh&eacute;t nếu chưa skincare hoặc b&ocirc;i kem chống nắng đ&uacute;ng c&aacute;ch. Xem link sau để biết c&aacute;ch b&ocirc;i kem chống nắng La Roche Posay Anthelios Anti-Shine Dry Touch Gel Cream SPF 50+ kh&ocirc;ng bị v&oacute;n cục nh&eacute;:<br />\r\nhttps://www.youtube.com/watch?v=snrOQOz-50Y</em></p>\r\n\r\n<p><em>#gelcream #chongnanglaroche #seenshop #antheliosantiImperfection #dadau #thiendau #laroche #posay #kemchongnanglaroche #dadaumun #danhaycam #damun #larocheposay</em></p>\r\n', 4, 731, 110, 0, 790000, 790000, '2019-06-03 22:33:51', '1', '2022-05-20 17:03:10', '1', 1, 1),
(15, 6, 'Kem Chống Nắng Dr.Skin Care SJMmedical', 'kem-chong-nang-dr-skin-care-sjmmedical', 'drskin.jpg', 'drskin.jpg', 'Kem Chống Nắng Dr.Skin Care SJMmedical Anti-Uv Perfect Sunscreen SPF 50+ PA++++', '<p><em>Kem Chống Nắng Dr.Skin Care SJMmedical Anti-Uv Perfect Sunscreen SPF 50+ PA++++&nbsp;</em></p>\r\n\r\n<p><em>Th&agrave;nh phần:</em></p>\r\n\r\n<p><em>- Chiết xuất th&agrave;nh phần thi&ecirc;n nhi&ecirc;n l&agrave;nh t&iacute;nh từ l&aacute; tr&agrave;, hoa c&uacute;c,&hellip;kh&ocirc;ng chỉ gi&uacute;p dưỡng da m&agrave; c&ograve;n tạo được hiệu ứng căng b&oacute;ng nhẹ, gi&uacute;p da trở n&ecirc;n trong suốt v&agrave; rạng rỡ hơn<br />\r\n- Hyaluronic Acid cấp ẩm v&agrave; dưỡng ẩm tối ưu&nbsp;<br />\r\n- Vitamin B3, vitamin C cung cấp c&aacute;c vitamin cần thiết cho da</em></p>\r\n\r\n<p><em>Cộng dụng :</em></p>\r\n\r\n<p><em>- Bảo vệ da dưới &aacute;nh nắng mặt trời v&agrave; c&aacute;c tia UVA/UVB&nbsp;<br />\r\n- C&oacute; t&aacute;c dụng thay thế kem l&oacute;t trang điểm&nbsp;<br />\r\n- Cung cấp nhiều dưỡng chất cần thiết cho da&nbsp;<br />\r\n- Hỗ trợ dưỡng da s&aacute;ng hồng tự nhi&ecirc;n</em></p>\r\n\r\n<p><em>Hướng dẫn sử dụng:</em></p>\r\n\r\n<p><em>- Lấy một lượng sản phẩm vừa đủ (bằng đồng xu) chấm đều l&ecirc;n khu&ocirc;n mặt, sau đ&oacute; massage v&agrave; vỗ nhẹ để sản phẩm thấm thấu</em></p>\r\n\r\n<p><em>LƯU &Yacute; KHI ĐĂT H&Agrave;NG:<br />\r\n- Khi nhận h&agrave;ng bạn kiểm tra tem ni&ecirc;m phong đơn h&agrave;ng v&agrave; quay lại video clip từ khi bắt đầu mở hộp v&agrave; kiểm tra sản phẩm b&ecirc;n trong kh&ocirc;ng bị cắt đoạn. Nếu Lam Thảo c&oacute; sai s&oacute;t giao thiếu/sai sản phẩm (thiếu số lượng/sản phẩm, sai sản phẩm,&hellip;) bạn vui l&ograve;ng nhắn tin shopee hoặc gọi Hotline b&ecirc;n m&igrave;nh sẽ giải quyết v&agrave; hỗ trợ gửi bổ sung đầy đủ h&agrave;ng. Ngoại trừ video clip, b&ecirc;n m&igrave;nh sẽ kh&ocirc;ng thể hỗ trợ giải quyết những vấn đề sai s&oacute;t được n&ecirc;n mong bạn th&ocirc;ng cảm v&agrave; hợp t&aacute;c c&ugrave;ng Lam Thảo nh&eacute; &amp;lt;3</em></p>\r\n', 4, 120, 7, 20, 250000, 200000, '2019-06-03 22:36:43', '1', '2022-05-20 17:04:45', '1', 1, 1),
(16, 11, 'Bảng phấn trang điểm PINKFLASH', 'bang-phan-trang-diem-pinkflash', 'sonli.jpg', 'sonli.jpg', 'Bảng phấn trang điểm PINKFLASH có thể kết hợp màu trang điểm cho mặt/mắt 120g', '<p>M&Ocirc; TẢ SẢN PHẨM</p>\r\n\r\n<p>Thời gian giao h&agrave;ng dự kiến cho sản phẩm n&agrave;y l&agrave; từ 7-9 ng&agrave;y</p>\r\n\r\n<p>Pinkflash OhMyLove</p>\r\n\r\n<p>M&agrave;u sắc mới 100% phong ph&uacute; với nhiều chức năng kh&aacute;c nhau</p>\r\n\r\n<p>Bảng phấn trang điểm cho khu&ocirc;n mặt, bao gồm phấn mắt, phấn m&aacute;, đường viền v&agrave; phấn bắt s&aacute;ng , đ&acirc;y l&agrave; một bảng m&agrave;u linh hoạt, tiện lợi v&agrave; hiệu quả.</p>\r\n\r\n<p>Bột mịn như nhung, tạo cảm gi&aacute;c nhẹ nh&agrave;ng m&agrave; kh&ocirc;ng l&agrave;m da bị tổn thường v&agrave; dễ phối m&agrave;u, sắc tố cao với m&agrave;u sắc đẹp chỉ với một thao t&aacute;c đơn giản.</p>\r\n\r\n<p>C&oacute; 10 m&agrave;u trong một bảng m&agrave;u, nhiều m&agrave;u ph&ugrave; hợp để trang điểm trong nhiều dịp kh&aacute;c nhau, với nhiều m&agrave;u sắc kh&aacute;c nhau gi&uacute;p bạn tạo ra nhiều kiểu trang điểm mỗi ng&agrave;y.</p>\r\n\r\n<p>C&aacute;c nh&atilde;n hiệu kh&aacute;c: Sắc tố thấp &rarr; Ch&uacute;ng t&ocirc;i: Sắc tố cao</p>\r\n\r\n<p>C&aacute;c nh&atilde;n hiệu kh&aacute;c: &Iacute;t m&agrave;u hơn &rarr; Ch&uacute;ng t&ocirc;i: 4 trong 1 v&agrave; 10 m&agrave;u</p>\r\n\r\n<p>3 loại:</p>\r\n\r\n<p>1 Mandarin Latte</p>\r\n\r\n<p>2 đ&aacute; d&acirc;u</p>\r\n\r\n<p>3 kẹo bơ cứng caramen</p>\r\n\r\n<p>Điểm nổi bật</p>\r\n\r\n<p>Kh&ocirc;ng dễ phai</p>\r\n\r\n<p>Một bảng m&agrave;u gồm 10 m&agrave;u</p>\r\n\r\n<p>Phấn mềm v&agrave; mịn</p>\r\n\r\n<p>Phấn kh&ocirc;ng dễ bị mốc</p>\r\n\r\n<p>Sắc tố cao</p>\r\n\r\n<p>Dễ d&agrave;ng kết hợp c&aacute;c m&agrave;u sắc</p>\r\n\r\n<p>D&ugrave;ng được cho mắt v&agrave; mặt</p>\r\n\r\n<p>Sử dụng với kem nền PINKFLASH để tạo lớp trang điểm ho&agrave;n hảo</p>\r\n\r\n<p>Sử dụng:</p>\r\n\r\n<p>1. Chọn phấn mắt y&ecirc;u th&iacute;ch của bạn v&agrave; thoa l&ecirc;n mắt.</p>\r\n\r\n<p>2. Để c&oacute; đường viền đẹp hơn, h&atilde;y thoa phấn l&ecirc;n hai b&ecirc;n mũi, dưới đường viền h&agrave;m v&agrave; dọc theo chiều huyệt m&aacute;.</p>\r\n\r\n<p>3. Thoa phấn m&aacute; l&ecirc;n g&ograve; m&aacute; để tạo phấn m&aacute; hồng tự nhi&ecirc;n.</p>\r\n\r\n<p>4. Để ho&agrave;n thiện vẻ ngo&agrave;i, h&atilde;y thoa phấn bắt s&aacute;ng l&ecirc;n m&aacute; v&agrave; sống mũi để l&agrave;m nổi bật điểm nhấn.</p>\r\n\r\n<p>Thương hiệu: pinkflash</p>\r\n\r\n<p>K&iacute;ch thước: Ti&ecirc;u chuẩn</p>\r\n\r\n<p>Chống nước: C&oacute;</p>\r\n\r\n<p>Th&iacute;ch hợp cho mọi loại da</p>\r\n\r\n<p>Vấn đề về da: Kh&ocirc;ng.</p>\r\n\r\n<p>Xuất xứ từ Trung Quốc</p>\r\n\r\n<p>M&agrave;u sắc sản phẩm: 3 m&agrave;u để bạn lựa chọn</p>\r\n\r\n<p>M&atilde; sản phẩm: PF-M02</p>\r\n\r\n<p>Lưu &yacute; khi sử dụng:</p>\r\n\r\n<p>1.Kh&ocirc;ng sử dụng cho c&aacute;c mục đ&iacute;ch kh&aacute;c ngo&agrave;i trang điểm.</p>\r\n\r\n<p>2.Để xa tầm tay trẻ em.</p>\r\n\r\n<p>Bảo quản: Lưu trữ ở nơi tho&aacute;ng m&aacute;t</p>\r\n\r\n<p>Hạn sử dụng: 3 năm</p>\r\n\r\n<p>Th&agrave;nh phần:</p>\r\n\r\n<p>Mica, TALC, ATHYLHEXYL PALMITATE, MAGNESIUM STEARATE, MINERAL OIL, VIMETHICONE, SYNTHETIC FLUORPHLOGOPITE, ISONONANONANOATE, CALCIUM SODIUM BOROSISISC, WALYPOLYPOLYPOLYPOLYPOLY, WALCIUM SODIUM BOROSISISC, PONLYPOLYPOLYPOLYPOLYPOLYPOLY, PONLY PONELPOLY, PONLY PONLYPOLY, PONLY PONLYPOLY, PONLY PONLYPOLY, PONLY PONLYPOLY, PONLY PONLYPOLY, PONLY PONLYPOLY, PONLY PONLY PONLYPOLY, PONLY PONLYPOLY, PONLY PONLYPO, PONLYPOLY PONLYPOLY, PONLYPOLY PONLYPOLYPOLYPOLYPOLY Polypos, POLYNEL, POLYPOLYNEL, POLYPOLYNEXLYNEXY, POS, POLYPOLYNEL, POLYPOLYPROY, POLYPOLY, POLYPROY, POLYNEL, POLYNEL, POLYNEL, POLYNEL, POLYNEL, POLYNEL, POLYNEL, POLYNEL, POLYNEL, POLYNEL, PONEL, PONEL, PONEL</p>\r\n\r\n<p>C&oacute; thể bao gồm: CI 77891, CI 77491, CI 19140, CI 77499, CI 15850, CI 77492, CI 45410, CI 77266, CI 75470, CI 16035</p>\r\n', 5, 61, 6, 10, 568000, 398000, '2022-05-20 15:26:52', '1', '2022-05-20 15:23:03', '1', 1, 1),
(19, 9, 'Son môi Flower Knows', 'son-moi-flower-knows', 'son.jpg', 'son.jpg', 'Son môi Flower Knows lâu trôi thiết kế vỏ hình thiên thần nhỏ sang trọng 14 màu sắc tự chọn 4g', '<p><em>Thời gian giao h&agrave;ng dự kiến cho sản phẩm n&agrave;y l&agrave; từ 7-9 ng&agrave;y</em></p>\r\n\r\n<p><em>Thiết kế son m&ocirc;i h&igrave;nh thi&ecirc;n thần nhỏ</em></p>\r\n\r\n<p><em>Thương hiệu: Flower Knows</em></p>\r\n\r\n<p><em>Thiết kế thi&ecirc;n thần nghệ thuật bắt mắt</em></p>\r\n\r\n<p><em>Kết cấu mịn như nhung cho vẻ ngo&agrave;i bắt mắt v&agrave; sang trọng</em></p>\r\n\r\n<p><em>Nhẹ nh&agrave;ng, duy&ecirc;n d&aacute;ng, ngọt ng&agrave;o v&agrave; tươi m&aacute;t</em></p>\r\n\r\n<p><em>Lưu &yacute;.</em></p>\r\n\r\n<p><em>Đặc điểm:</em></p>\r\n\r\n<p><em>1) Thiết kế (Thi&ecirc;n thần Cupid)</em></p>\r\n\r\n<p><em>2) Kết cấu mịn như nhung (h&igrave;nh thức sang trọng)</em></p>\r\n\r\n<p><em>3) Mượt v&agrave; nhiều t&ocirc;ng m&agrave;u (thay đổi ngoại h&igrave;nh linh hoạt)</em></p>\r\n\r\n<p><em>M&agrave;u sắc:</em></p>\r\n\r\n<p><em># B01rebel Angel (Đỏ cam)</em></p>\r\n\r\n<p><em># B02 Liberty Angel (Eapple Red)</em></p>\r\n\r\n<p><em># B03 Dawn Angel (Đỏ gạch)</em></p>\r\n\r\n<p><em># B04 Moonlight Angel (M&agrave;u đỏ mạnh với t&ocirc;ng m&agrave;u xanh lam)</em></p>\r\n\r\n<p><em># B05 Twin Angel (Caramen latte)</em></p>\r\n\r\n<p><em># W01 Thi&ecirc;n thần c&oacute; sừng (Tr&agrave; sữa đ&agrave;o)</em></p>\r\n\r\n<p><em># W02 Covenant Angel (Covenant )</em></p>\r\n\r\n<p><em># W03 Savior Angel (Hoa hồng kh&ocirc;)</em></p>\r\n\r\n<p><em># W04 King&#39;s Angel (Rượu vang anh đ&agrave;o đỏ)</em></p>\r\n\r\n<p><em># W05 Tribunal Angel (Đỏ đất s&eacute;t n&acirc;u)</em></p>\r\n\r\n<p><em># L01 Guardian Angel (Tr&agrave; sữa hạnh nh&acirc;n)</em></p>\r\n\r\n<p><em># L02 Wish Angel (Hồng đ&agrave;o hồng)</em></p>\r\n\r\n<p><em># L03 Cofession Angel (Cofession angel)</em></p>\r\n\r\n<p><em># L04 Suspire Angel (N&acirc;u tr&agrave;)</em></p>\r\n\r\n<p><em>Hướng dẫn sử dụng:<br />\r\nBước 1: Chăm s&oacute;c da m&ocirc;i: d&ugrave;ng son dưỡng m&ocirc;i để thoa l&ecirc;n hai m&ocirc;i, đợi 5 ph&uacute;t rồi thoa đều.&nbsp;<br />\r\nBước 2: Để tr&aacute;nh son bị lem ra khỏi đường viền m&ocirc;i, bạn c&oacute; thể d&ugrave;ng kem che khuyết điểm để l&agrave;m mờ đường viền m&ocirc;i, sau đ&oacute; d&ugrave;ng ch&igrave; kẻ m&ocirc;i để tạo đường viền m&ocirc;i.<br />\r\nBước 3: Thoa son v&agrave; d&ugrave;ng ng&oacute;n tay t&aacute;n đều, m&agrave;u sẽ l&acirc;u tr&ocirc;i hơn. D&ugrave;ng ng&oacute;n tay th&igrave; kh&ocirc;ng d&ugrave;ng cọ hoặc c&aacute;c loại kh&aacute;c, nếu kh&ocirc;ng m&agrave;u son sẽ kh&ocirc;ng đẹp. D&ugrave;ng đầu cọ t&aacute;n nhẹ từ kh&oacute;e m&ocirc;i đến giữa m&ocirc;i bằng ng&oacute;n tay.&nbsp;<br />\r\nBước 4: Nếu th&iacute;ch độ ẩm c&oacute; thể thoa đều son. Nếu bạn th&iacute;ch hiệu ứng l&igrave;, bạn c&oacute; thể d&ugrave;ng giấy lụa để thấm son, m&agrave;u son sẽ đẹp. C&oacute; t&aacute;c dụng cao, bạn sẽ cảm thấy tuyệt vời<br />\r\nLưu &yacute; rằng: độ b&atilde;o h&ograve;a m&agrave;u rất cao n&ecirc;n rất dễ thoa son, cẩn thận kh&ocirc;ng thoa son l&ecirc;n đường viền m&ocirc;i v&agrave; kh&ocirc;ng bị d&iacute;nh răng.</em></p>\r\n\r\n<p><em>Lưu &yacute;:<br />\r\nH&igrave;nh ảnh ba chiều c&oacute; thể hơi kh&aacute;c v&igrave; kỹ thuật sản xuất l&agrave; b&igrave;nh thường.</em></p>\r\n\r\n<p><em>Vỏ của son m&ocirc;i n&agrave;y c&oacute; hiệu ứng ba chiều bằng laser. V&igrave; m&agrave;u cơ bản n&ecirc;n mỗi sản phẩm c&oacute; thể c&oacute; hiệu ứng ba chiều kh&aacute;c nhau n&ecirc;n điều n&agrave;y l&agrave; b&igrave;nh thường v&agrave; sẽ kh&ocirc;ng ảnh hưởng đến m&agrave;u sắc của son m&ocirc;i.</em></p>\r\n\r\n<p><em>C&aacute;c mẫu chỉ mang t&iacute;nh chất tham khảo, son m&ocirc;i c&oacute; thể hiển thị c&aacute;c m&agrave;u kh&aacute;c nhau theo c&aacute;c nguồn s&aacute;ng, thiết bị, m&agrave;n h&igrave;nh v&agrave; t&ocirc;ng m&agrave;u da kh&aacute;c nhau. Ch&uacute;ng t&ocirc;i đ&atilde; cố gắng hết sức để điều chỉnh nguồn s&aacute;ng để ph&ugrave; hợp với m&agrave;u sắc sản phẩm thực tế nhất c&oacute; thể. Xin lưu &yacute; điều n&agrave;y trước khi mua!</em></p>\r\n\r\n<p><br />\r\n<em>【Th&ocirc;ng Tin sản phẩm】<br />\r\nThời hạn sử dụng: 3 năm<br />\r\nKhối lượng tịnh: 4g<br />\r\nSản xuất tại Trung Quốc<br />\r\nXuất xứ: nh&agrave; m&aacute;y của ch&uacute;ng t&ocirc;i sản xuất</em></p>\r\n', 5, 140, 22, 10, 13890000, 11890000, '2019-06-04 23:30:36', '1', '2022-05-20 15:24:59', '1', 1, 1),
(20, 5, 'Dung dịch cân bằng dưỡng ẩm và làm dịu da Bioderma', 'dung-dich-can-bang-duong-am-va-lam-diu-da-bioderma', 'biodema.jpg', 'biodema.jpg', 'Dung dịch cân bằng dưỡng ẩm và làm dịu da Bioderma Sensibio Tonique 250ml', '<p>M&Ocirc; TẢ SẢN PHẨM</p>\r\n\r\n<p>Dung dịch Sensibio Tonique c&acirc;n bằng độ pH sinh l&iacute; tự nhi&ecirc;n, dưỡng ẩm v&agrave; l&agrave;m dịu da nhạy cảm</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>ĐỐI TƯỢNG SỬ DỤNG:</p>\r\n\r\n<p>D&agrave;nh cho mọi loại da, đặc biệt l&agrave; da nhạy cảm v&agrave; k&eacute;m dung nạp</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>ĐỘ AN TO&Agrave;N:</p>\r\n\r\n<p>Kh&ocirc;ng g&acirc;y k&iacute;ch ứng</p>\r\n\r\n<p>Kh&ocirc;ng g&acirc;y mụn</p>\r\n\r\n<p>Kh&ocirc;ng chứa paraben v&agrave; hương liệu</p>\r\n\r\n<p>Dung nạp tối ưu cho da</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>T&Aacute;C ĐỘNG SINH HỌC:</p>\r\n\r\n<p>- Chiết xuất dưa leo v&agrave; Allantoin gi&uacute;p l&agrave;m dịu da</p>\r\n\r\n<p>- Glycerine gi&uacute;p cung cấp độ ẩm cho da</p>\r\n\r\n<p>- Phức hợp tự nhi&ecirc;n DAF (Dermatological Advanced Formulation - C&ocirc;ng thức Chăm s&oacute;c da Cao Cấp) gi&uacute;p tăng ngưỡng dung nạp cho l&agrave;n da nhạy cảm nhất.</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>HƯỚNG DẪN SỬ DỤNG:</p>\r\n\r\n<p>- Sử dụng cho da mặt v&agrave; cổ ngay sau khi rửa sạch (tốt nhất l&agrave; sau khi d&ugrave;ng Sensibio H2O)</p>\r\n\r\n<p>- Thấm ướt b&ocirc;ng cotton bằng dung dịch Sensibio Tonique v&agrave; massage nhẹ nh&agrave;ng để l&agrave;n da sạch bụi bẩn v&agrave; mềm mịn chuẩn bị cho c&aacute;c bước chăm s&oacute;c da tiếp theo.</p>\r\n\r\n<p>- Kh&ocirc;ng cần rửa lại với nước.</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>TH&Agrave;NH PHẦN:</p>\r\n\r\n<p>AQUA/WATER/EAU, GLYCERIN, XYLITOL, POLYSORBATE 20, MANNITOL, RHAMNOSE, FRUCTOOLIGOSACCHARIDES, CUCUMIS SATIVUS (CUCUMBER) FRUIT EXTRACT, PEG-40 HYDROGENATED CASTOR OIL, ALLANTOIN, PROPYLENE GLYCOL, DISODIUM EDTA, CETRIMONIUM BROMIDE, SODIUM HYDROXIDE, CITRIC ACID.</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>VỀ BIODERMA:</p>\r\n\r\n<p>L&agrave; một trong những h&atilde;ng dược mỹ phẩm ti&ecirc;n phong đến từ Ph&aacute;p, th&agrave;nh lập từ những năm 1970, BIODERMA đ&atilde; đạt được những th&agrave;nh tựu độc đ&aacute;o khi ứng dụng c&aacute;c th&agrave;nh phần v&agrave; c&ocirc;ng nghệ sinh học v&agrave;o việc chăm s&oacute;c v&agrave; chữa trị c&aacute;c bệnh về da. C&aacute;c sản phẩm của BIODERMA m&ocirc; phỏng cơ chế sinh học tự nhi&ecirc;n của l&agrave;n da dựa tr&ecirc;n c&aacute;c th&agrave;nh phần v&agrave; phương thức hoạt động của ch&uacute;ng, sử dụng nguồn nguy&ecirc;n liệu tinh khiết nhất từ những th&agrave;nh phần tự nhi&ecirc;n c&oacute; sẵn trong tế b&agrave;o da, từ đ&oacute; gi&uacute;p da dung nạp tốt hơn, cải thiện v&agrave; phục hồi l&agrave;n da nhưng vẫn t&ocirc;n trọng qu&aacute; tr&igrave;nh tự nhi&ecirc;n của da.</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>Dung t&iacute;ch: 250ml</p>\r\n\r\n<p>Xuất xứ thương hiệu: Ph&aacute;p</p>\r\n\r\n<p>Nơi sản xuất: Ph&aacute;p</p>\r\n\r\n<p>Hạn sử dụng: 3 năm kể từ ng&agrave;y sản xuất.</p>\r\n\r\n<p>Hướng dẫn bảo quản: Nơi tho&aacute;ng m&aacute;t, tr&aacute;nh &aacute;nh nắng trực tiếp, đậy nắp k&iacute;n sau khi sử dụng</p>\r\n\r\n<p>Bao b&igrave; thay đổi theo từng đợt nhập h&agrave;ng.</p>\r\n', 1, 300, 26, 10, 700000, 630000, '2019-06-04 23:33:43', '1', '2022-05-20 15:17:49', '1', 1, 1),
(21, 1, 'Set kem dưỡng ẩm, làm sáng Laneige', 'set-kem-duong-am-lam-sang-laneige', 'chamd.jpg', 'chamda.jpg', 'Set kem dưỡng ẩm, làm sáng Laneige 5 trong 1 chăm sóc da', 'Bài viết đang được cập nhật', 6, 240, 17, 12, 780000, 683700, '2022-05-20 16:33:16', '1', '2022-05-20 00:00:00', '2', 0, 1),
(22, 9, 'Son Kem Perfect Diary ReadMe Lip Veil', 'son-kem-perfect-diary-readme-lip-veil', 'son.jpg', 'sonkem.jpg', 'Son Kem Perfect Diary ReadMe Lip Veil Chất Kem Nhung Lì Lâu Trôi 14 Màu Tùy Chọn 4g', '<p>Thời gian giao h&agrave;ng dự kiến cho sản phẩm n&agrave;y l&agrave; từ 7-9 ng&agrave;y</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>Chi tiết:</p>\r\n\r\n<p>M&agrave;u sắc sản phẩm</p>\r\n\r\n<p>001 Trust me</p>\r\n\r\n<p>002 Nice to meet you</p>\r\n\r\n<p>003 Don&#39;t judge me</p>\r\n\r\n<p>004 Look at me</p>\r\n\r\n<p>005 Leave me alone</p>\r\n\r\n<p>006 Hug me</p>\r\n\r\n<p>007 Call me</p>\r\n\r\n<p>008 Long time no see</p>\r\n\r\n<p>101 I got you</p>\r\n\r\n<p>102 Anytime</p>\r\n\r\n<p>103 Because of you</p>\r\n\r\n<p>104 You deserve better</p>\r\n\r\n<p>105 Live young</p>\r\n\r\n<p>106 Let it go</p>\r\n\r\n<p>013 It&#39;s you</p>\r\n\r\n<p>014 Fancy seeing you</p>\r\n\r\n<p>113 Sweet talk to me</p>\r\n\r\n<p>114 Crush on you</p>\r\n\r\n<p>201 I&rsquo;m good</p>\r\n\r\n<p>202 Whatever</p>\r\n\r\n<p>203 Love me or never</p>\r\n\r\n<p>204 Feel dxxn good</p>\r\n\r\n<p>205 Try me</p>\r\n\r\n<p>206 Take a risk</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>M&atilde; sản phẩm: PDB266</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>M&ocirc; tả Sản phẩm</p>\r\n\r\n<p>Thương hiệu: Perfect Diary</p>\r\n\r\n<p>Sản phẩm: SON M&Ocirc;I NHUNG MỀM NHẸ NH&Agrave;NG PERFECT DIARY</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>Đặc điểm ch&iacute;nh</p>\r\n\r\n<p>#ReadMe l&agrave; d&ograve;ng sản phẩm khẳng định tạo n&ecirc;n phong c&aacute;ch cho đ&ocirc;i m&ocirc;i bạn</p>\r\n\r\n<p>Thiết kế giống như một tấm thẻ, chất son nhung-l&igrave;, son l&ecirc;n rất nhẹ nh&agrave;ng</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>Đặc điểm sản phẩm</p>\r\n\r\n<p>1. Lấy cảm hứng từ những chiếc thẻ name card: Thiết kế đặc biệt giống một chiếc thẻ ghi t&ecirc;n, h&atilde;y sử dụng chiếc son m&ocirc;i như bản sắc ri&ecirc;ng của bạn.</p>\r\n\r\n<p>2. N&oacute;i theo c&aacute;ch của bạn: C&acirc;u n&oacute;i tượng trưng khắc ghi con người thật của bạn.</p>\r\n\r\n<p>3. Kết cấu mềm mịn như nhung v&agrave; mỏng nhẹ: Tự nhi&ecirc;n t&ocirc;n l&ecirc;n đ&ocirc;i m&ocirc;i xinh của bạn.</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>Th&agrave;nh phần:</p>\r\n\r\n<p>1.Polydimethylsiloxan,</p>\r\n\r\n<p>2. Polyme li&ecirc;n kết ch&eacute;o Polydimethylsiloxan,</p>\r\n\r\n<p>3. Isododecane</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>Hướng dẫn sử dụng</p>\r\n\r\n<p>Bắt đầu ở một g&oacute;c của m&ocirc;i v&agrave; k&eacute;o cọ đến giữa m&ocirc;i. Sau đ&oacute;, bắt đầu ở g&oacute;c kh&aacute;c v&agrave; từ từ k&eacute;o cọ v&agrave;o giữa để nối đường kẻ của bạn. T&ocirc; m&agrave;u l&ecirc;n m&ocirc;i v&agrave; t&aacute;n đều.</p>\r\n\r\n<p>&nbsp;</p>\r\n\r\n<p>Hướng dẫn bảo quản từ cửa h&agrave;ng:</p>\r\n\r\n<p>Tr&aacute;nh đặt sản phẩm ở nhiệt độ cao v&agrave; &aacute;nh s&aacute;ng trực tiếp, tr&aacute;nh xa tầm tay trẻ em.</p>\r\n\r\n<p>Ng&agrave;y hết hạn: 3 năm</p>\r\n\r\n<p>Trọng lượng tịnh: 4g</p>\r\n\r\n<p>K&iacute;ch thước g&oacute;i h&agrave;ng: 40 * 88 * 14mm</p>\r\n', 5, 720, 161, 0, 250000, 250000, '2019-06-10 00:04:42', '1', '2022-05-20 15:16:24', '1', 1, 1),
(23, 1, '[COMBO] Kem Nám Tàn Nhang V9Queen', 'combo-kem-nam-tan-nhang-v9queen', 'spcda.jpg', 'spcda.jpg', '[COMBO] Kem Nám Tàn Nhang V9Queen, Bộ Sản Phẩm Chăm Sóc Da V9 Detox 30ml, 9Day Revival 10ml, 3Day Beauty 10g', '<p>L&agrave;n da mỗi ng&agrave;y đều phải trải qua chuỗi phản ứng g&acirc;y l&atilde;o h&oacute;a h&igrave;nh th&agrave;nh c&aacute;c vết th&acirc;m, sạm, n&aacute;m, t&agrave;n nhang do kh&ocirc;ng kh&iacute; bụi bẩn, &aacute;nh nắng mặt trời,&hellip; nhưng kh&ocirc;ng phải l&uacute;c n&agrave;o ch&uacute;ng ta cũng c&oacute; nhiều thời gian để thực hiện đủ c&aacute;c bước dưỡng thiết yếu gi&uacute;p da khỏe v&agrave; duy tr&igrave; sự tươi trẻ. C&ugrave;ng Mỹ phẩm V9Queen, lật tẩy 3 bước dưỡng da tối giản d&ugrave; bận mấy cũng c&oacute; thể dưỡng da 5 ph&uacute;t mỗi ng&agrave;y với COMBO 3 sản phẩm dưỡng da của V9QUEEN.</p>\r\n\r\n<p>TH&Agrave;NH PHẦN &amp;amp; C&Ocirc;NG DỤNG:&nbsp;<br />\r\n1 - MẶT NẠ THẢI ĐỘC &ndash; V9 DETOX với c&ocirc;ng thức đột ph&aacute; gi&agrave;u Vitamin E, Vitamin B3, Vitamin C, Collagen thủy ph&acirc;n, Hyaluronic acid, chiết xuất Khổ qua, Mộc qua, C&uacute;c la m&atilde;, D&acirc;u tằm, Tr&agrave; xanh, L&ocirc; hội gi&uacute;p loại bỏ tế b&agrave;o chết, bụi bẩn tr&ecirc;n da một c&aacute;ch nhẹ nh&agrave;ng, ngo&agrave;i ra c&ograve;n dưỡng ẩm, chống oxy h&oacute;a, giảm sắc tố sẫm m&agrave;u gi&uacute;p da tươi s&aacute;ng, rạng rỡ hơn&nbsp;</p>\r\n\r\n<p>2 - SERUM N&Aacute;M 9DAY REVIVAL công thức đột phá chứa tinh chất cô đặc của các hoạt chất: chiết xuất Sâm tố nữ, Cam thảo, Mầm hạt kê, Mật ong lên men, Lúa mạch lên men, Protein sữa ong chúa thủy phân, Rong sụn, Curcumin, Vitamin E, Alpha arbutin giúp mờ thâm nám, tàn nhang, cung cấp dưỡng chất, cấp ẩm và duy trì độ ẩm cho da, giúp da đều màu, sáng hồng, tươi tắn&nbsp;</p>\r\n\r\n<p>3 - KEM FACE DƯỠNG 3DAY BEAUTY: Tăng cường sức mạnh trẻ h&oacute;a l&agrave;n da, Dưỡng trắng da chuy&ecirc;n s&acirc;u, Ho&agrave;n thiện chu tr&igrave;nh dưỡng da của mọi c&ocirc; n&agrave;ng Cung cấp độ ẩm, gi&uacute;p dưỡng ẩm tự nhi&ecirc;n chuy&ecirc;n s&acirc;u, duy tr&igrave; độ ẩm l&acirc;u d&agrave;i. Chống oxy h&oacute;a, ph&aacute; hủy gốc tự do, l&agrave;m chậm qu&aacute; tr&igrave;nh l&atilde;o h&oacute;a da, l&agrave;m mờ dần c&aacute;c vết th&acirc;m n&aacute;m tr&ecirc;n da, dưỡng da trắng hồng, đều m&agrave;u. Chống nắng, bảo vệ da khỏi tia tử ngoại.<br />\r\nHƯỠNG DẪN SỬ DỤNG:&nbsp;</p>\r\n\r\n<p>MẶT NẠ V9 DETOX ( tuần 2 lần v&agrave;o buổi tối)&nbsp;<br />\r\n- Rửa sạch mặt, lấy 1 lượng kem vừa đủ t&aacute;n nhanh l&ecirc;n khắp da mặt (tr&aacute;nh v&ugrave;ng mắt, m&ocirc;i) masage li&ecirc;n tục&nbsp;<br />\r\n- Chỉ sau 3 - 5 ph&uacute;t tinh chất kem của mặt nạ sẽ thẩm thấu v&agrave; h&uacute;t hết bụi bẩn chất độc tr&ecirc;n bề mặt da.&nbsp;<br />\r\n- Rửa mặt lại với nước sạch&nbsp;</p>\r\n\r\n<p>SERUM (ng&agrave;y 2 lần s&aacute;ng tối)&nbsp;<br />\r\n- Rửa sạch da mặt.&nbsp;<br />\r\n- cho 2 giọt serum ra tay thoa l&ecirc;n những v&ugrave;ng da bị n&aacute;m, nếu bị hết da mặt th&igrave; đều l&ecirc;n mặt. Masage nhẹ nh&agrave;ng theo chiều hướng l&ecirc;n, ra ngo&agrave;i để serum được thẩm thấu tốt nhất v&agrave;o da.&nbsp;</p>\r\n\r\n<p>KEM FACE ( sử ng&agrave;y 1 lần buổi s&aacute;ng )<br />\r\n- L&agrave;m kh&ocirc; da v&agrave; chấm kem l&ecirc;n 5 điểm tr&ecirc;n mặt: tr&aacute;n, mũi, cằm, v&agrave; 2 b&ecirc;n m&aacute;&nbsp;<br />\r\n- T&aacute;n đều lớp mỏng kem vừa chấm l&ecirc;n bề mặt da v&agrave; vỗ nhẹ l&agrave;n da để kem thẩm thấu&nbsp;<br />\r\n*Lưu &yacute; nhỏ: v&igrave; l&agrave; kem dưỡng da ban ng&agrave;y v&agrave; c&oacute; t&iacute;nh chống nắng n&ecirc;n c&aacute;c bạn c&oacute; thể sử dụng như 1 lớp kem nền trước khi trang điểm<br />\r\nTH&Ocirc;NG TIN THƯƠNG HIỆU<br />\r\nThương hiệu: V9QUEEN<br />\r\nNơi sản xuất: Việt Nam<br />\r\nDung t&iacute;ch: 50ml<br />\r\nHạn sử dụng 3 năm kể từ ng&agrave;y sản xuất<br />\r\nTh&ocirc;ng tin sản xuất in tr&ecirc;n bao b&igrave; sản phẩm</p>\r\n\r\n<p>V9QUEEN l&agrave; một trong những thương hiệu mỹ phẩm b&aacute;n chạy tại việt nam. Trong v&ograve;ng hơn 2 năm, V9Queen đ&atilde; nhanh ch&oacute;ng được người ti&ecirc;u d&ugrave;ng tr&ecirc;n to&agrave;n quốc biết đến v&agrave; tin d&ugrave;ng nhờ cung cấp sản phẩm chất lượng, đa dạng gi&aacute; cả hợp l&yacute;. Với c&aacute;c d&ograve;ng sản phẩm chăm s&oacute;c c&aacute; nh&acirc;n như: kem dưỡng trắng da mặt, sữa rửa mặt ngừa mụn v&agrave; l&agrave;m trắng, serum giảm mụn mờ th&acirc;m n&aacute;m, gel tẩy tế b&agrave;o da chết, sữa tắm trắng da body, son trang điểm &amp;amp; dưỡng m&ocirc;i,...<br />\r\n- Sử dụng những nguy&ecirc;n liệu xanh sạch với nguồn gốc xuất xứ r&otilde; r&agrave;ng, chọn lọc.<br />\r\n- D&acirc;y chuyền sản xuất đạt chuẩn.<br />\r\n- V9Queen vinh hạnh nằm trong top 10 thương hiệu sao v&agrave;ng đất Việt 2020<br />\r\n- C&ocirc;ng thức v&agrave; c&ocirc;ng nghệ được chuyển giao bởi H&agrave;n Quốc.<br />\r\n- Sản phẩm chiết xuất từ thi&ecirc;n nhi&ecirc;n ph&ugrave; hợp v&agrave; l&agrave;nh t&iacute;nh với l&agrave;n da, an to&agrave;n sử dụng cho b&agrave; bầu, phụ nữ mang thai.</p>\r\n\r\n<p>Chăm s&oacute;c v&agrave; l&agrave;m đẹp cho những l&agrave;n da nhạy cảm nhất với c&aacute;c th&agrave;nh phần từ thi&ecirc;n nhi&ecirc;n ch&iacute;nh l&agrave; mục ti&ecirc;u m&agrave; V9QUEEN lu&ocirc;n hướng tới để trở n&ecirc;n tốt hơn mỗi ng&agrave;y</p>\r\n\r\n<p>V9QUEEN CAM KẾT&nbsp;<br />\r\n- ️Sản phẩm ph&acirc;n phối ch&iacute;nh h&atilde;ng 100%&nbsp;<br />\r\n- ️Date của Sản phẩm lu&ocirc;n l&agrave; mới nhất v&agrave; được kiểm tra kĩ c&agrave;ng, cẩn thận<br />\r\n- ️H&agrave;ng c&oacute; sẵn, giao h&agrave;ng ngay khi nhận được đơn&nbsp;<br />\r\n- ️Ho&agrave;n tiền đổi trả miễn ph&iacute; nếu giao sai sản phẩm hoặc sản phẩm lỗi do nh&agrave; sản xuất<br />\r\n- ️Hỗ trợ đổi trả theo quy định của Shopee Mall</p>\r\n\r\n<p>#serumnguanam, #tinh_chất_dưỡng_trắng_da &nbsp;#serum_dưỡng_trắng, #serum_ngừa _n&aacute;m, #kemduongda, #kem_face_dưỡng_trắng_da, #kem_ngừa_n&aacute;m, #kemchongnang, #v9detox #matnathaidoc #matnatrangda, #matnathaichi, #combotrangda, #combonamtannhang, #v9queenchinhhang, #myphamchinhhang, #v9queen, #trangda, #namtannhang</p>\r\n', 5, 470, 33, 15, 225000, 195000, '2019-06-10 00:07:42', '1', '2022-05-20 15:13:15', '1', 1, 1);
INSERT INTO `db_product` (`id`, `catid`, `name`, `alias`, `avatar`, `img`, `sortDesc`, `detail`, `producer`, `number`, `number_buy`, `sale`, `price`, `price_sale`, `created`, `created_by`, `modified`, `modified_by`, `trash`, `status`) VALUES
(24, 5, 'Nước Hoa Hồng Soothing Facial Toner Simple', 'nuoc-hoa-hong-soothing-facial-toner-simple', 'smile1.jpg', 'smile1.jpg', 'Nước Hoa Hồng Soothing Facial Toner Simple cân bằng ẩm cho da 200ml', '<p><em>TH&Ocirc;NG TIN THƯƠNG HI&Ecirc;̣U<br />\r\nSimple l&agrave; thương hiệu d&agrave;nh cho da nhạy cảm được ưa chuộng số 1 ở thị trường UK, sản phẩm được b&aacute;n hầu hết trong c&aacute;c hiệu thuốc, th&agrave;nh phần l&agrave;nh t&iacute;nh, l&agrave; sản phẩm ho&agrave;n hảo nhất cho l&agrave;n da nhạy cảm.&nbsp;</em></p>\r\n\r\n<p><em>V&Ecirc;̀ SẢN PH&Acirc;̉M<br />\r\nSimple Kind To Skin Soothing Facial Toner &nbsp;l&agrave; loại nước c&acirc;n bằng da kh&ocirc;ng chứa cồn được rất nhiều c&ocirc; g&aacute;i ưa th&iacute;ch lựa chọn nhờ khả năng l&agrave;m sạch, nu&ocirc;i dưỡng v&agrave; c&acirc;n bằng độ PH tốt cho da m&agrave; kh&ocirc;ng hề g&acirc;y k&iacute;ch ứng.</em></p>\r\n\r\n<p><em>Với c&ocirc;ng thức pha trộn kh&eacute;o l&eacute;o giữa c&aacute;c nguy&ecirc;n liệu thi&ecirc;n nhi&ecirc;n như chiết xuất c&acirc;y phỉ nổi tiếng trong việc s&aacute;t khuẩn cho da, cung cấp độ ẩm l&agrave;m mềm da v&agrave; l&agrave;m se kh&iacute;t lỗ ch&acirc;n l&ocirc;ng một c&aacute;ch đ&aacute;ng kể.&nbsp;</em></p>\r\n\r\n<p><em>THÀNH PH&Acirc;̀N VÀ C&Ocirc;NG DỤNG<br />\r\nĐ&uacute;ng như ti&ecirc;u ch&iacute; của h&atilde;ng n&agrave;y l&agrave; đơn giản n&ecirc;n c&aacute;c th&agrave;nh phần c&oacute; trong sản phẩm cũng cực k&igrave; nhẹ dịu v&agrave; l&agrave;nh t&iacute;nh như:<br />\r\n- Hoạt chất Witch Hazel từ nước c&acirc;y phỉ gi&uacute;p se kh&iacute;t lỗ ch&acirc;n l&ocirc;ng tự nhi&ecirc;n, s&aacute;t khuẩn, l&agrave;m mềm v&agrave; mịn da<br />\r\n- Hoạt chất Chamomile chiết xuất từ hoa c&uacute;c gi&uacute;p chống vi&ecirc;m v&agrave; l&agrave;m dịu da.&nbsp;<br />\r\n- Pro-Vitamin B5 gi&uacute;p da phục hồi, mềm v&agrave; mịn m&agrave;ng.<br />\r\n- Nước hoa hồng Simple Toner gi&uacute;p loại bỏ lớp trang điểm v&agrave; bụi bẩn c&ograve;n s&oacute;t lại sau qu&aacute; tr&igrave;nh rửa mặt, đồng thời gi&uacute;p l&agrave;n da được cung cấp độ ẩm tức thời, trở nhanh ch&oacute;ng trở về độ pH c&acirc;n bằng để sẵn s&agrave;ng cho c&aacute;c bước dưỡng tiếp theo.</em></p>\r\n\r\n<p><em>CÁCH DÙNG<br />\r\n- Toner sẽ hoạt đ&ocirc;̣ng t&ocirc;́t nh&acirc;́t sau bước sữa rửa mặt. Có nhi&ecirc;̀u cách bạn có th&ecirc;̉ apply toner m&ocirc;̣t cách hi&ecirc;̣u quả như:<br />\r\nC&aacute;ch 1: Bạn c&oacute; thể đổ toner ra tay, xoa nhẹ, rồi vỗ toner l&ecirc;n mặt.<br />\r\nC&aacute;ch 2: Bạn cũng c&oacute; thể đổ toner ra b&ocirc;ng cotton để lau nhẹ toner l&ecirc;n da mặt. Lau theo hướng từ trong ra ngoài, từ dưới l&ecirc;n tr&ecirc;n đ&ecirc;̉ hạn ch&ecirc;́ tạo ra n&ecirc;́p nhăn tr&ecirc;n da.<br />\r\nSau đó bạn có th&ecirc;̉ ti&ecirc;́p tục các bước dưỡng da ti&ecirc;́p theo.</em></p>\r\n\r\n<p><em>Hạn sử dụng: 2024<br />\r\n3 năm kể từ ng&agrave;y sản xuất.<br />\r\nHạn d&ugrave;ng kh&ocirc;ng in tr&ecirc;n bao b&igrave; sản phẩm. Chi tiết inbox shop hướng dẫn c&aacute;ch check khi nhận h&agrave;ng.<br />\r\nXu&acirc;́t xứ: UK<br />\r\nDung tích: 200ml</em></p>\r\n\r\n<p><em>#Toner #nuochoahong #simple #duongam #danhaycam #damun #canbangam</em></p>\r\n', 5, 250, 3, 50, 160000, 80000, '2019-06-10 00:10:34', '1', '2022-05-20 14:53:20', '1', 1, 1),
(25, 2, 'Thuốc Nhuộm Tóc Màu NÂU TRÀ SỮA/ NÂU KHÓI', 'thuoc-nhuom-toc-mau-nau-tra-sua-nau-khoi', 'tnhuom4.jpg', 'tnhuom4.jpg', 'Thuốc Nhuộm Tóc Màu NÂU TRÀ SỮA/ NÂU KHÓI - LÊN TỪ NỀN ĐEN + Tặng Kèm Oxy Trợ Dưỡng', '<p>&nbsp;</p>\r\n\r\n<p><em><a href=\"https://cellphones.com.vn/mobile/samsung/galaxy-note.html\" target=\"_blank\"><strong>✅N&Acirc;U KH&Oacute;I / N&Acirc;U TR&Agrave; SỮA - L&Ecirc;N TỪ NỀN ĐEN V&Agrave; N&Acirc;U KH&Ocirc;NG CẦN TẨY T&Oacute;C<br />\r\n-TỰ NHUỘM T&Oacute;C TẠI NH&Agrave; CỰC ĐƠN GIẢN VỚI BEAUTYHAIR:<br />\r\n????????????<br />\r\n???? L&ecirc;n từ base t&oacute;c đen v&agrave; n&acirc;u l&agrave; chuẩn kh&ocirc;ng cần chỉnh.<br />\r\n???? Tặng ngay k&egrave;m trợ oxy, găng tay, hướng dẫn nhuộm t&oacute;c tại nh&agrave;, bạn cần mua th&ecirc;m lược nhuộm, dưỡng t&oacute;c hay sản phẩm kh&aacute;c th&igrave; mua th&ecirc;m trong Shop nh&eacute; !<br />\r\n???? Thuốc nhuộm si&ecirc;u thơm đ&atilde; được khử m&ugrave;i Amoniac.<br />\r\n???? Cam kết h&agrave;ng ch&iacute;nh h&atilde;ng 100% - l&ecirc;n m&agrave;u chuẩn mịn.<br />\r\n???? Freeship To&agrave;n Quốc đơn từ 400K.<br />\r\n➖➖➖➖➖➖➖➖➖➖➖➖➖<br />\r\n▶️ HƯỚNG DẪN NHUỘM:<br />\r\nb1: pha tu&yacute;p thuốc N&Acirc;U KH&Oacute;I với oxi rồi chải đều từ ch&acirc;n ra ngọn rồi masage t&oacute;c cho đều sau đ&oacute; d&ugrave;ng nilon bọc k&iacute;n để 50-55&rsquo;&nbsp;<br />\r\nb2: kiểm tra xem l&ecirc;n đều m&agrave;u rồi xả sạch v&agrave; thoa đều dầu xả cho mượt t&oacute;c sau đ&oacute; xả sạch rồi sấy kh&ocirc;&nbsp;</strong></a></em></p>\r\n\r\n<p><em><a href=\"https://cellphones.com.vn/mobile/samsung/galaxy-note.html\" target=\"_blank\"><strong>▶️ Nếu l&ecirc;n từ nền đen sẽ l&ecirc;n m&agrave;u hơi trầm hơn ch&uacute;t nh&eacute;. Nếu l&ecirc;n từ nền t&oacute;c tẩy lvl 8-10 sẽ l&ecirc;n m&agrave;u na n&aacute; x&aacute;m kh&oacute;i n&ecirc;n c&aacute;c n&agrave;ng lưu &yacute; trước khi nhuộm nhen.</strong></a></em></p>\r\n\r\n<p><em><a href=\"https://cellphones.com.vn/mobile/samsung/galaxy-note.html\" target=\"_blank\"><strong>▶️ M&agrave;u t&oacute;c được nhiều bạn trẻ ưa chuộng v&igrave; sự trẻ trung c&aacute; t&iacute;nh v&agrave; v&ocirc; c&ugrave;ng &quot;T&acirc;y&quot; đấy nh&eacute;.</strong></a></em></p>\r\n\r\n<p><em><a href=\"https://cellphones.com.vn/mobile/samsung/galaxy-note.html\" target=\"_blank\"><strong>▶️ Kh&ocirc;ng h&egrave; k&eacute;n da, đi l&agrave;m, đi chơi đều được hết n&egrave;. Đặc biệt l&agrave; kh&ocirc;ng cần phải tẩy t&oacute;c đ&acirc;u c&aacute;c n&agrave;ng nh&eacute;.</strong></a></em></p>\r\n\r\n<p><em><a href=\"https://cellphones.com.vn/mobile/samsung/galaxy-note.html\" target=\"_blank\"><strong>▶️ C&ograve;n đợi g&igrave; m&agrave; kh&ocirc;ng inbox ngay cho #BeautyHair để được tư vấn về m&agrave;u t&oacute;c n&agrave;y nh&eacute;. Hong ưng hong t&iacute;nh xiền ❤️<br />\r\n➖➖➖➖➖➖➖➖➖➖➖➖➖<br />\r\n#thuocnhuomtoc #thuocnhuomtocgiare #thuocnhuomtoc85k #thuocnhuomtoc90k #thuocnhuomtoc150k #n&acirc;ukh&oacute;i #n&acirc;ukh&oacute;is&aacute;ng #naukhoi #n&acirc;u_caramel #n&acirc;ubơ #n&acirc;ucoffee #n&acirc;uv&agrave;ngkh&oacute;i #n&acirc;ulạnh #n&acirc;ut&acirc;y #n&acirc;usữa #n&acirc;utr&agrave;sữa #n&acirc;usươngm&ugrave; #n&acirc;u kh&oacute;i #ốpđiệnthoại #&ocirc;mai #quần&aacute;o #v&aacute;y #son #giầy #dầugội #sữatắm #sữarửamặt #dưỡngda #mặtnạ #thuốcnhuộmt&oacute;c #t&oacute;cm&agrave;u</strong></a></em></p>\r\n\r\n<p>&nbsp;</p>\r\n', 1, 367, 5, 12, 60000, 50000, '2019-06-10 00:14:46', '1', '2022-05-20 14:50:35', '1', 1, 1),
(27, 2, 'Thuốc nhuộm tóc Nâu Tây Ánh Khói ', 'thuoc-nhuom-toc-nau-tay-anh-khoi', 'tnhuom2.jpg', 'tnhuom2.jpg', 'Thuốc nhuộm tóc Nâu Tây Ánh Khói - Lên Từ Nền Đen Và Nâu, Không Cần Tẩy', '<p>✅ M&Agrave;U N&Acirc;U T&Acirc;Y KH&Oacute;I l&ecirc;n từ nền Đen v&agrave; N&acirc;u, Kh&ocirc;ng Cần Tẩy T&oacute;c.&nbsp;<br />\r\nTỰ NHUỘM T&Oacute;C TẠI NH&Agrave; CỰC ĐƠN GIẢN VỚI BEAUTYHAIR:</p>\r\n\r\n<p>????M&agrave;u N&acirc;u T&acirc;y Kh&oacute;i - M&agrave;u được săn đ&oacute;n nhiều nhất bởi c&aacute;c chị em v&igrave; l&agrave; m&agrave;u rất t&ocirc;n da, độ s&aacute;ng vừa tầm m&agrave; lại rất T&acirc;y n&ecirc;n được rất nhiều c&aacute;c chị em y&ecirc;u th&iacute;ch lựa chọn !<br />\r\n-1 Set bao gồm: 1 tu&yacute;p thuốc nhuộm 100ml + 1 lọ trợ nhuộm oxy 100ml + tặng k&egrave;m gang tay, hướng dẫn nhuộm t&oacute;c tại nh&agrave;, bạn cần mua th&ecirc;m lược nhuộm, dưỡng t&oacute;c hay sản phẩm kh&aacute;c th&igrave; bạn mua th&ecirc;m trong Shop nh&eacute; !<br />\r\n???????? Hướng dẫn sử dụng:<br />\r\n- Đeo găng tay nilon m&igrave;nh c&oacute; tặng c&aacute;c cậu trong đơn<br />\r\n- Sử dụng ngay cho t&oacute;c kh&ocirc; hoặc xả t&oacute;c ẩm<br />\r\n- Trộn thuốc nhuộm v&agrave;o kem ph&aacute;t triển m&agrave;u.&nbsp;<br />\r\n- B&ocirc;i trực tiếp l&ecirc;n to&agrave;n bộ t&oacute;c c&aacute;ch ch&acirc;n 2cm v&agrave; ch&ugrave;m Nilon rồi để ủ 40&rsquo; sau đ&oacute; chải phần ch&acirc;n t&oacute;c để th&ecirc;m 10&rsquo; rồi xả sạch, d&ugrave;ng dầu xả thoa đều cho mềm t&oacute;c rồi xả sạch v&agrave; sấy kh&ocirc; .<br />\r\n- Thời gian xử l&yacute;:<br />\r\n&bull; 40 ph&uacute;t để phủ m&agrave;u ho&agrave;n to&agrave;n cho t&oacute;c đen hoặc t&oacute;c tự nhi&ecirc;n.<br />\r\n&bull; 60 -70 ph&uacute;t d&agrave;nh cho t&oacute;c kh&oacute; l&ecirc;n m&agrave;u.<br />\r\n- Sau khi xả kem nhuộm bằng nước sạch, kh&ocirc;ng cần d&ugrave;ng th&ecirc;m dầu gội.<br />\r\nCh&uacute;c c&aacute;c cậu nhuộm th&agrave;nh c&ocirc;ng, Love all<br />\r\n&bull;&nbsp;<br />\r\n#nhuomtoc #kemnhuom #duongtoc &nbsp;#Nhuommaukhoi #thuocnhuomtoc #maukhoi&nbsp;<br />\r\n#thuốcnhuộmt&oacute;cnam&nbsp;<br />\r\n#thuốcnhuộmt&oacute;cm&agrave;un&acirc;ukh&oacute;i<br />\r\n#thuốcnhuộmt&oacute;cm&agrave;un&acirc;uhạt dẻ<br />\r\n#thuốcnhuộmt&oacute;cm&agrave;uv&agrave;ngs&aacute;ng<br />\r\n#thuốcnhuộmt&oacute;cgi&aacute;rẻ<br />\r\n#thuốcnhuộmt&oacute;cm&agrave;u xanh dương<br />\r\n#thuốcnhuộmt&oacute;cm&agrave;un&acirc;uv&agrave;ng<br />\r\n#thuốcnhuộmt&oacute;cm&agrave;uxanhdươngđen<br />\r\n#thuốcnhuộmt&oacute;cm&agrave;un&acirc;ut&acirc;y #m&agrave;un&acirc;ut&acirc;y #t&oacute;cm&agrave;uN&acirc;uT&acirc;y #nhuộm t&oacute;c #Kinglovehairs #freeshipxtra<br />\r\n&nbsp;</p>\r\n', 5, 430, 54, 0, 50000, 50000, '2019-06-14 21:46:25', '1', '2022-05-20 15:56:37', '1', 1, 1),
(28, 2, 'Thuốc nhuộm nâu ánh khói', 'thuoc-nhuom-nau-anh-khoi', 'tnhuom.jpg', 'tnhuom.jpg', 'Thuốc nhuộm nâu ánh khói, tinh chất thảo dược thiên nhiên', '<p>Thuốc Nhuộm T&oacute;c N&acirc;u Kh&oacute;i S&aacute;ng | L&ecirc;n Từ Nền T&oacute;c N&acirc;u Kh&ocirc;ng Tẩy - Minzihair<br />\r\n????????????????????????????????????????????????????<br />\r\n???? L&ecirc;n từ base t&oacute;c n&acirc;u tự nhi&ecirc;n hoặc n&acirc;ng tone (level 5-7).<br />\r\n???? Tặng ngay k&egrave;m trợ oxy, găng &amp;amp; mũ nilon hữu cơ!!<br />\r\n???? Thuốc nhuộm si&ecirc;u thơm đ&atilde; được khử m&ugrave;i Amoniac.<br />\r\n???? Cam kết h&agrave;ng ch&iacute;nh h&atilde;ng 100% - l&ecirc;n m&agrave;u chuẩn mịn.<br />\r\n???? Lưu &yacute;: NỀN C&Agrave;NG S&Aacute;NG TH&Igrave; &Aacute;NH KH&Oacute;I C&Agrave;NG NHIỀU &amp;amp; &Aacute;NH N&Acirc;U C&Agrave;NG &Iacute;T. Đặc biệt c&aacute;c bạn t&oacute;c tẩy cần lưu &yacute; khi nhuộm m&agrave;u n&agrave;y v&igrave; sẽ ra &aacute;nh x&aacute;m kh&oacute;i nha.&nbsp;<br />\r\n➖➖➖➖➖➖➖➖➖➖➖➖➖<br />\r\n▶️ N&acirc;u kh&oacute;i s&aacute;ng l&ecirc;n đẹp nhất l&agrave; từ nền n&acirc;u lvl 5-6 c&aacute;c bạn nh&oacute;e.</p>\r\n\r\n<p>▶️ Nếu l&ecirc;n từ nền đen sẽ ra n&acirc;u lạnh hoặc n&acirc;u lạnh hoặc n&acirc;u kh&oacute;i th&ocirc;i n&egrave;. Nếu l&ecirc;n từ nền t&oacute;c tẩy lvl 8-10 sẽ l&ecirc;n m&agrave;u na n&aacute; x&aacute;m kh&oacute;i n&ecirc;n c&aacute;c n&agrave;ng lưu &yacute; trước khi nhuộm nhen.</p>\r\n\r\n<p>▶️ M&agrave;u t&oacute;c n&acirc;u kh&oacute;i s&aacute;ng &nbsp;được rất nhiều bạn trẻ ưa chuộng v&igrave; sự trẻ trung c&aacute; t&iacute;nh v&agrave; v&ocirc; c&ugrave;ng &quot;T&acirc;y&quot; đấy nh&eacute;. Đ&acirc;y l&agrave; một trong số những m&agrave;u tone n&acirc;u si&ecirc;u hot của năm nay. S&aacute;nh vai với n&acirc;u tr&agrave; sữa, n&acirc;u kh&oacute;i lạnh, n&acirc;u t&acirc;y...</p>\r\n\r\n<p>▶️ Kh&ocirc;ng h&egrave; k&eacute;n da, đi l&agrave;m, đi chơi đều được hết n&egrave;. Đặc biệt l&agrave; kh&ocirc;ng cần phải tẩy t&oacute;c đ&acirc;u c&aacute;c n&agrave;ng nh&eacute;.</p>\r\n\r\n<p>▶️ C&ograve;n đợi g&igrave; m&agrave; kh&ocirc;ng Direct/inbox ngay cho @Minzihair để được tư vấn về m&agrave;u t&oacute;c n&agrave;y nh&eacute;. Hong ưng hong t&iacute;nh xiền ❤️<br />\r\n➖➖➖➖➖➖➖➖➖➖➖➖➖<br />\r\n#thuocnhuomtoc #thuocnhuomtocgiare #thuocnhuomtoc80k #thuocnhuomtoc90k #thuocnhuomtoc150k #minzihair #minzihairs #chenglovehairs #buddyhairs_fb #n&acirc;ukh&oacute;i #n&acirc;ukh&oacute;is&aacute;ng #naukhoi #n&acirc;u_caramel #n&acirc;ubơ #n&acirc;ucoffee #n&acirc;uv&agrave;ngkh&oacute;i #n&acirc;ulạnh #n&acirc;ut&acirc;y</p>\r\n', 5, 320, 310, 15, 80000, 60000, '2022-05-20 00:00:00', '1', '2022-05-20 15:58:33', '1', 1, 1),
(30, 3, 'Bộ Dầu Gội Weilaiya Chính Hãng DGG171', 'bo-da-u-go-i-weilaiya-chinh-hang-dgg171', 'daugoi7.jpg', 'daugoi7.jpg', 'Dầu gội bóng mượt', '<p>Bộ Dầu Gội Gừng Weilaiya DGG171 Dầu Gội Đầu Nữ Cặp Gừng H&agrave; Thủ &Ocirc; Ngăn Rụng Gi&uacute;p Mọc T&oacute;c Từ Thảo Dược Thi&ecirc;n Nhi&ecirc;n<br />\r\nTH&Ocirc;NG TIN SẢN PHẨM<br />\r\n&nbsp; - Thương hiệu nổi bật trong ng&agrave;nh t&oacute;c quốc tế: WEILAIYA &ndash; D&ograve;ng sản phẩm cao cấp đ&atilde; được phủ s&oacute;ng to&agrave;n cầu.<br />\r\n&nbsp; - Sản xuất bởi: Guangzhou WeilaiYa Biology Technology Co., Ltd - Room 3401, No.888, Yuncheng West Road, Baiyun District, Guangzhou City, China Nhập khẩu &amp;amp; chịu tr&aacute;ch nhiệm ph&acirc;n phối bởi: C&ocirc;ng ty TNHH Wonder Union -C&ocirc;ng ty TNHH Wonder Union - Số 6 Phố Kim Đồng, Phường Gi&aacute;p B&aacute;t, Quận Ho&agrave;ng Mai, H&agrave; Nội<br />\r\n&nbsp; - Hạn sử dụng: 3 năm (shop lu&ocirc;n đảm bảo date mới nhất cho kh&aacute;ch y&ecirc;u của shop)<br />\r\n&nbsp;<br />\r\nD&Ugrave;NG L&Agrave; TH&Iacute;CH M&Ecirc;<br />\r\n&nbsp; - M&ugrave;i gừng thơm nhẹ, tạo bọt vừa đủ. L&agrave; d&ograve;ng dầu gội ngăn rụng t&oacute;c, gi&uacute;p nang t&oacute;c mọc nhanh hơn, giảm rụng t&oacute;c hữu hiệu.<br />\r\n&nbsp; - T&oacute;c tẩy, hư, nhuộm th&igrave; kh&ocirc;ng n&ecirc;n bỏ qua d&ograve;ng dầu gội phục hồi n&agrave;y. Si&ecirc;u phục hồi t&oacute;c hư tổn, gi&uacute;p t&oacute;c su&ocirc;n mượt, giữ hương thơm l&acirc;u tr&ecirc;n t&oacute;c.<br />\r\n&nbsp; - Kh&ocirc;ng chỉ mang lại một da đầu sạch dầu, giảm ngứa da đầu, sạch gầu m&agrave; c&ograve;n gi&uacute;p thư gi&atilde;n tinh thần, bạn hẫy biến 15 ph&uacute;t gội đầu trở th&agrave;nh khoảng thời gian relax cực mạnh, vực dậy tinh thần sau một ng&agrave;y l&agrave;m việc căng thẳng, mệt mỏi.<br />\r\n&nbsp; - Dầu gội mượt t&oacute;c, sạch gầu, bồng bềnh, giữ bạn nổi bật trong đ&aacute;m đ&ocirc;ng kh&ocirc;ng chỉ bởi vẻ quyến rủ của m&aacute;i t&oacute;c m&agrave; c&ograve;n từ hương thơm tinh khiết của n&oacute;.<br />\r\n&nbsp; - H&agrave; thủ &ocirc; c&oacute; trong dầu gội gi&uacute;p t&oacute;c đen &oacute;ng ả hơn.</p>\r\n\r\n<p>TH&Agrave;NH PHẦN DẦU GỘI THI&Ecirc;N NHI&Ecirc;N<br />\r\n&nbsp; - Dầu gội thảo dược 100% tự nhi&ecirc;n, l&agrave;nh t&iacute;nh v&agrave; an to&agrave;n với mọi đối tượng sử dụng (người gi&agrave;, mẹ bầu, trẻ em,...)<br />\r\n&nbsp; - Chiết suất gừng tươi: Chứa hơn 12 th&agrave;nh phần, hoạt chống chống oxy h&oacute;a như B1, B6, chất b&eacute;o, tinh bột, sắt, canxi,&hellip;l&agrave; những dưỡng chất tốt cho việc phục hồi hư tổn t&oacute;c.<br />\r\n&nbsp; - Chiết suất H&agrave; Thủ &Ocirc;: L&agrave; một trong những loại thảo dược tốt cho sức khỏe con người, đặc biệt l&agrave; c&ocirc;ng dụng chống rụng t&oacute;c v&agrave; l&agrave;m đen t&oacute;c hiệu quả.<br />\r\n&nbsp; - Hoa oải hương Lavender: L&agrave;m sạch da đầu, c&acirc;n bằng da đầu, ngăn ngừa rụng t&oacute;c v&agrave; k&iacute;ch th&iacute;ch tuần ho&agrave;n m&aacute;u l&ecirc;n đầu, gia tăng độ đ&agrave;n hồi cho t&oacute;c.<br />\r\n&nbsp; - Dầu Olive: Chứa nhiều th&agrave;nh phần c&oacute; lợi như axit oleic, axit palmitic v&agrave; squalene gi&uacute;p l&agrave;m mềm v&agrave; giữ độ ẩm cho t&oacute;c. Những chất n&agrave;y kh&ocirc;ng chỉ l&agrave;m mềm t&oacute;c b&ecirc;n ngo&agrave;i m&agrave; c&ograve;n th&acirc;m nhập v&agrave;o s&acirc;u b&ecirc;n trong gi&uacute;p t&oacute;c chắc khỏe.<br />\r\n&nbsp; - Protein Hydrolase: Phục hồi nang t&oacute;c hư tổn, cải thiện ch&acirc;n t&oacute;c chắc khỏe hơn, giảm thiểu rụng t&oacute;c.</p>\r\n\r\n<p>KH&Aacute;CH H&Agrave;NG N&Agrave;O CẦN SỬ DỤNG<br />\r\n&nbsp; - Người t&oacute;c thưa, yếu, g&atilde;y rụng, h&oacute;i.<br />\r\n&nbsp; - Da đầu nhiều dầu nhờn, nhanh bẩn, bết t&oacute;c.<br />\r\n&nbsp; - T&oacute;c nhiều g&agrave;u, kh&ocirc;, ngứa kh&oacute; chịu.<br />\r\n&nbsp; - T&oacute;c kh&ocirc; chẻ ngọn, hư tổn sau tẩy, nhuộm.</p>\r\n\r\n<p>CH&Iacute;NH S&Aacute;CH ĐỔI TRẢ BẢO H&Agrave;NH<br />\r\n&nbsp; - Lu&ocirc;n đảm bảo date mới nhất.<br />\r\n&nbsp; - Đổi trả trong 3 ng&agrave;y nếu h&agrave;ng lỗi hoặc giao nhầm.<br />\r\n&nbsp; - Cam kết h&agrave;ng ch&iacute;nh h&atilde;ng (giấy chứng nhận ph&acirc;n phối từ c&ocirc;ng ty)</p>\r\n\r\n<p>#daugoi #daugoinganrungtoc #daugoidau #daugoiphuchoi #goithaoduoc #daugoingangau #daugoisagung #daugoikichthichmoctoc #nganrungtoc #dauxaweilaiya #combogoixa #daugoidauduongtoc #daugoidanhchotocdau #daugoingannguarungtoc #daugoigiuptocnhanhdai #phuchoitoc #daugoigau #daugoinam</p>\r\n', 5, 57, 0, 10, 180000, 120000, '2022-05-20 14:42:55', '1', '2022-05-20 15:45:12', '69', 1, 1),
(31, 9, 'Son Kem Lì Sgirls by M.O.I ', 'son-kem-li-sgirls-by-m-o-i', '948c889d47d342615ccfaf3438a68a5e.jpg', '0bcd25fa89a30710d9cfa177d56c12a7.jpg', 'Son Kem Lì Sgirls by M.O.I chất nhung lì mềm môi nhiều dưỡng 4g', '<p>&nbsp; &nbsp; BIỂN - M&Ugrave;A H&Egrave; V&Agrave; S.GIRLS BY M.O.I SUMMER EDITION&nbsp;<br />\r\n&nbsp; &nbsp; Biển v&agrave; M&ugrave;a H&egrave; l&agrave; nguồn cảm hứng ch&iacute;nh cho bộ sưu tập son sắp ra mắt của M.O.I v&agrave;o ng&agrave;y 10/07/2021. Kh&ocirc;ng những mang th&ocirc;ng điệp t&iacute;ch cực &ldquo;D&ugrave; c&oacute; thế n&agrave;o, h&egrave; đến rồi, rực rỡ l&ecirc;n th&ocirc;i!&rdquo; (Anything happens, summer is coming, just shining!), bộ sưu tập S.Girls by M.O.I Summer Edition c&ograve;n gi&uacute;p mọi người khỏa lấp cơn&hellip; th&egrave;m &ldquo;vitamin sea&rdquo; trong thời điểm n&agrave;y.<br />\r\n&nbsp; &nbsp;&nbsp;<br />\r\n&nbsp; &nbsp;Son c&oacute; 6 m&agrave;u chủ đạo đặt theo t&ecirc;n của 6 địa danh du lịch biển nổi tiếng Việt Nam<br />\r\n1. HALONG &nbsp; : HỒNG BABY<br />\r\n2.DANANG. &nbsp; : HỒNG MẬN<br />\r\n3.NHATRANG. &nbsp; : CAM SAN H&Ocirc;<br />\r\n4.MUINE. &nbsp; : CAM ĐỎ<br />\r\n5.LYSON. &nbsp; : CAM NUDE<br />\r\n6.PHUQUOC. &nbsp; : CAM T&Acirc;Y<br />\r\n&nbsp; &nbsp;<br />\r\n&nbsp; &nbsp; Trong đ&oacute; c&oacute; 2 loại set hộp 3 thỏi tặng k&egrave;m t&uacute;i du lịch chống nước<br />\r\n1. SET 3.1: HỒNG BABY, CAM SAN H&Ocirc;, CAM ĐỎ(1,3,4)<br />\r\n2. SET 3.2 : HỒNG MẬN, CAM NUDE, CAM T&Acirc;Y(2,5,6)<br />\r\n&nbsp; &nbsp; &nbsp;<br />\r\n&nbsp; &nbsp; &nbsp; &nbsp;Th&agrave;nh phần: Dimethicone, Dimethicone Crosspolymer, Diisostearyl Malate, Titanium Dioxide, Polyglyceryl-2 Triisostearate, Tribehenin, CI 77492, Cetyl PEG/PPG-10/1 Dimethicone, VP/Hexadecene Copolymer, CI 19140, CI 15850, Polyhydroxystearic Acid, Polyglyceryl-2 Diisostearate, Ethylhexyl Palmitate, Isopropyl Myristate, Isostearic Acid, Lecithin, Parfum, CI 17200, Polyglyceryl-3 Polyricinoleate, Dehydroacetic Acid, Hydrolyzed Hyaluronic Acid.<br />\r\n&nbsp; &nbsp; &nbsp;&nbsp;<br />\r\n&nbsp; &nbsp; &nbsp; &nbsp;Trọng lượng : 4g<br />\r\n&nbsp; &nbsp; &nbsp;&nbsp;<br />\r\n&nbsp; &nbsp; &nbsp; &nbsp; Hướng dẫn sử dụng:<br />\r\n&nbsp; Lấy một &iacute;t son m&ocirc;i v&agrave; nhẹ nh&agrave;ng thao bằng cọ cong mềm để c&oacute; được lớp son l&igrave; như sương. Lập đi v&agrave;i lần để c&oacute; m&agrave;u đậm hơn.<br />\r\n&nbsp; Son kem l&agrave; một kết cấu đặc biệt. Nếu m&ocirc;i bị v&oacute;n cục, bạn c&oacute; thể d&ugrave;ng ng&oacute;n tay thoa nhẹ để tạo lớp sương như nhung.<br />\r\n&nbsp;&nbsp;<br />\r\n&nbsp; 【Lưu &yacute;】 Nếu da m&ocirc;i của bạn bị kh&ocirc;, n&ecirc;n sử dụng son dưỡng l&agrave;m lớp nền cho m&ocirc;i. (Để cho ra kết quả tốt hơn, h&atilde;y lau sạch lớp dưỡng m&ocirc;i của bạn 5 ph&uacute;t sau khi thoa, sau đ&oacute; sử dụng lớp son m&ocirc;i, Bạn sẽ nhận c&oacute; được lớp son m&ocirc;i đ&aacute;ng kinh ngạc）</p>\r\n\r\n<p>Thương Hiệu: M.O.I Cosmetics</p>\r\n\r\n<p>Nh&agrave; Ph&acirc;n Phối Ch&iacute;nh H&atilde;ng: ĐẸP+ Store<br />\r\nHotline: 0877261887<br />\r\nĐịa Chỉ Store : 406 Nguyễn Tr&atilde;i, Trung Văn, Nam Từ Li&ecirc;m, H&agrave; Nội<br />\r\n#son #sonmoi #sonmoihongocha #sonkemli #sonkemlisgirl #sonsgirl #sonkemsgirl #sonmoisgirl #moicosmetics #sonkemmoi #depstore #hongocha&nbsp;</p>\r\n\r\n<p><br />\r\n&nbsp;</p>\r\n', 5, 40, 0, 20, 150000, 130000, '2022-05-20 15:43:58', '1', '2022-05-22 20:59:43', '69', 1, 1);

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_province`
--

CREATE TABLE `db_province` (
  `id` int(5) NOT NULL,
  `name` varchar(100) CHARACTER SET utf8 NOT NULL,
  `type` varchar(30) CHARACTER SET utf8 NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ROW_FORMAT=DYNAMIC;

--
-- Đang đổ dữ liệu cho bảng `db_province`
--

INSERT INTO `db_province` (`id`, `name`, `type`) VALUES
(1, 'Thành phố Hà Nội', 'Thành phố Trung ương'),
(2, 'Tỉnh Hà Giang', 'Tỉnh'),
(4, 'Tỉnh Cao Bằng', 'Tỉnh'),
(6, 'Tỉnh Bắc Kạn', 'Tỉnh'),
(8, 'Tỉnh Tuyên Quang', 'Tỉnh'),
(10, 'Tỉnh Lào Cai', 'Tỉnh'),
(11, 'Tỉnh Điện Biên', 'Tỉnh'),
(12, 'Tỉnh Lai Châu', 'Tỉnh'),
(14, 'Tỉnh Sơn La', 'Tỉnh'),
(15, 'Tỉnh Yên Bái', 'Tỉnh'),
(17, 'Tỉnh Hoà Bình', 'Tỉnh'),
(19, 'Tỉnh Thái Nguyên', 'Tỉnh'),
(20, 'Tỉnh Lạng Sơn', 'Tỉnh'),
(22, 'Tỉnh Quảng Ninh', 'Tỉnh'),
(24, 'Tỉnh Bắc Giang', 'Tỉnh'),
(25, 'Tỉnh Phú Thọ', 'Tỉnh'),
(26, 'Tỉnh Vĩnh Phúc', 'Tỉnh'),
(27, 'Tỉnh Bắc Ninh', 'Tỉnh'),
(30, 'Tỉnh Hải Dương', 'Tỉnh'),
(31, 'Thành phố Hải Phòng', 'Thành phố Trung ương'),
(33, 'Tỉnh Hưng Yên', 'Tỉnh'),
(34, 'Tỉnh Thái Bình', 'Tỉnh'),
(35, 'Tỉnh Hà Nam', 'Tỉnh'),
(36, 'Tỉnh Nam Định', 'Tỉnh'),
(37, 'Tỉnh Ninh Bình', 'Tỉnh'),
(38, 'Tỉnh Thanh Hóa', 'Tỉnh'),
(40, 'Tỉnh Nghệ An', 'Tỉnh'),
(42, 'Tỉnh Hà Tĩnh', 'Tỉnh'),
(44, 'Tỉnh Quảng Bình', 'Tỉnh'),
(45, 'Tỉnh Quảng Trị', 'Tỉnh'),
(46, 'Tỉnh Thừa Thiên Huế', 'Tỉnh'),
(48, 'Thành phố Đà Nẵng', 'Thành phố Trung ương'),
(49, 'Tỉnh Quảng Nam', 'Tỉnh'),
(51, 'Tỉnh Quảng Ngãi', 'Tỉnh'),
(52, 'Tỉnh Bình Định', 'Tỉnh'),
(54, 'Tỉnh Phú Yên', 'Tỉnh'),
(56, 'Tỉnh Khánh Hòa', 'Tỉnh'),
(58, 'Tỉnh Ninh Thuận', 'Tỉnh'),
(60, 'Tỉnh Bình Thuận', 'Tỉnh'),
(62, 'Tỉnh Kon Tum', 'Tỉnh'),
(64, 'Tỉnh Gia Lai', 'Tỉnh'),
(66, 'Tỉnh Đắk Lắk', 'Tỉnh'),
(67, 'Tỉnh Đắk Nông', 'Tỉnh'),
(68, 'Tỉnh Lâm Đồng', 'Tỉnh'),
(70, 'Tỉnh Bình Phước', 'Tỉnh'),
(72, 'Tỉnh Tây Ninh', 'Tỉnh'),
(74, 'Tỉnh Bình Dương', 'Tỉnh'),
(75, 'Tỉnh Đồng Nai', 'Tỉnh'),
(77, 'Tỉnh Bà Rịa - Vũng Tàu', 'Tỉnh'),
(79, 'Thành phố Hồ Chí Minh', 'Thành phố Trung ương'),
(80, 'Tỉnh Long An', 'Tỉnh'),
(82, 'Tỉnh Tiền Giang', 'Tỉnh'),
(83, 'Tỉnh Bến Tre', 'Tỉnh'),
(84, 'Tỉnh Trà Vinh', 'Tỉnh'),
(86, 'Tỉnh Vĩnh Long', 'Tỉnh'),
(87, 'Tỉnh Đồng Tháp', 'Tỉnh'),
(89, 'Tỉnh An Giang', 'Tỉnh'),
(91, 'Tỉnh Kiên Giang', 'Tỉnh'),
(92, 'Thành phố Cần Thơ', 'Thành phố Trung ương'),
(93, 'Tỉnh Hậu Giang', 'Tỉnh'),
(94, 'Tỉnh Sóc Trăng', 'Tỉnh'),
(95, 'Tỉnh Bạc Liêu', 'Tỉnh'),
(96, 'Tỉnh Cà Mau', 'Tỉnh');

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_slider`
--

CREATE TABLE `db_slider` (
  `id` int(11) NOT NULL,
  `name` varchar(255) CHARACTER SET utf8 NOT NULL,
  `link` varchar(255) CHARACTER SET utf8 NOT NULL,
  `img` varchar(100) CHARACTER SET utf8 NOT NULL,
  `created` datetime NOT NULL,
  `created_by` varchar(50) CHARACTER SET utf8 NOT NULL DEFAULT 'Supper Admin',
  `modified` datetime NOT NULL,
  `modified_by` varchar(50) CHARACTER SET utf8 NOT NULL DEFAULT 'Supper Admin',
  `trash` tinyint(1) NOT NULL DEFAULT 1,
  `status` tinyint(1) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_slider`
--

INSERT INTO `db_slider` (`id`, `name`, `link`, `img`, `created`, `created_by`, `modified`, `modified_by`, `trash`, `status`) VALUES
(3, 'trang chu 1', 'trang-chu', 'default.png', '2019-07-03 17:05:52', '1', '2019-07-03 17:07:18', '', 0, 0),
(4, 'slider trang chu 2', 'slider-trang-chu-2', 'iphone-banner.jpg', '2019-07-03 17:06:38', '1', '2019-07-03 17:06:38', '1', 0, 1),
(5, 'slider trang chu 3', 'slider-trang-chu-3', 'xsmaxbanner.png', '2019-07-03 17:06:58', '1', '2019-07-03 17:06:58', '1', 0, 1),
(7, 'slider2', 'slider2', '3.png', '2022-05-19 23:44:37', '1', '2022-05-19 23:44:37', '1', 1, 1),
(8, 'slider3', 'slider3', '4.png', '2022-05-19 23:45:10', '1', '2022-05-19 23:45:10', '1', 1, 1),
(9, 'slider4', 'slider4', 'slide2.jpg', '2022-05-19 23:45:39', '1', '2022-05-19 23:45:39', '1', 1, 1),
(10, 'slider4', 'slider4', 'slide1.jpg', '2022-05-19 23:45:57', '1', '2022-05-19 23:45:57', '1', 1, 1);

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_user`
--

CREATE TABLE `db_user` (
  `id` int(11) NOT NULL,
  `fullname` varchar(255) CHARACTER SET utf8 NOT NULL,
  `username` varchar(225) CHARACTER SET utf8 NOT NULL,
  `password` varchar(64) CHARACTER SET utf8 NOT NULL,
  `role` int(11) NOT NULL,
  `email` varchar(255) CHARACTER SET utf8 NOT NULL,
  `gender` int(1) NOT NULL,
  `phone` varchar(15) CHARACTER SET utf8 NOT NULL,
  `address` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `img` varchar(255) CHARACTER SET utf8 NOT NULL,
  `created` datetime NOT NULL,
  `trash` int(1) NOT NULL DEFAULT 1,
  `status` int(1) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_user`
--

INSERT INTO `db_user` (`id`, `fullname`, `username`, `password`, `role`, `email`, `gender`, `phone`, `address`, `img`, `created`, `trash`, `status`) VALUES
(1, 'ADMIN', 'admin', '7c4a8d09ca3762af61e59520943dc26494f8941b', 1, 'admin@gmail.com', 1, '0167892615', 'Gò vấp', 'user-group.png', '2019-04-23 09:16:16', 1, 1),
(2, 'Quản lý', 'quanly', 'a7d579ba76398070eae654c30ff153a4c273272a', 1, 'quanly@gmail.com', 1, '0985657410', 'Hà Nội', 'bc0d4c186522764fc9457b7bacb635e4.png', '2019-04-25 22:08:18', 1, 1),
(3, 'Nhân viên', 'nhanvien', '7c4a8d09ca3762af61e59520943dc26494f8941b', 2, 'nv@gmail.com', 1, '09990990', 'Gò vấp', 'b78af1dc3e1098f71e7cd607c49f5d51.png', '2019-04-23 09:20:41', 1, 1),
(4, 'Nguyễn Văn A', 'nhanviena', '7c222fb2927d828af22f592134e8932480637c0d', 2, 'nv@gmail.com', 1, '0985657410', 'Thanh Hóa', '403ceb0ed6fdb72494bbd2ac39182b04.png', '2019-04-25 22:08:18', 1, 1),
(6, 'Nguyễn Văn B', 'nhanvienb', '7c4a8d09ca3762af61e59520943dc26494f8941b', 2, 'nv@gmail.com', 0, '09876543532', 'Gò vấp', 'user.png', '2019-04-23 09:18:18', 1, 1),
(7, 'Nguyễn Đức Lương', 'luong', '7c4a8d09ca3762af61e59520943dc26494f8941b', 2, 'luong@gmail.com', 0, '23523523423', 'Gò vấp', '5eab74ad301795ffdfaa82f52579f71d.png', '2019-06-13 22:15:15', 0, 1),
(8, 'Ngô Văn Hiệp', 'hiep1998', '7c4a8d09ca3762af61e59520943dc26494f8941b', 2, 'vanhiepp1998@gmail.com', 0, '981643651', 'Gò vấp', 'default.png', '2019-06-13 22:30:03', 0, 1);

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `db_usergroup`
--

CREATE TABLE `db_usergroup` (
  `id` int(11) NOT NULL,
  `name` varchar(255) CHARACTER SET utf8 NOT NULL,
  `created` datetime NOT NULL,
  `created_by` int(11) NOT NULL,
  `modified` datetime NOT NULL,
  `modified_by` int(11) NOT NULL,
  `trash` tinyint(1) NOT NULL DEFAULT 1,
  `access` tinyint(1) NOT NULL DEFAULT 1,
  `status` tinyint(1) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Đang đổ dữ liệu cho bảng `db_usergroup`
--

INSERT INTO `db_usergroup` (`id`, `name`, `created`, `created_by`, `modified`, `modified_by`, `trash`, `access`, `status`) VALUES
(1, 'Toàn quyền', '2019-05-14 23:29:15', 1, '2019-05-14 23:29:15', 4, 1, 1, 1),
(2, 'Nhân viên', '2019-05-14 23:29:21', 1, '2019-05-14 23:29:21', 4, 1, 1, 1);

--
-- Chỉ mục cho các bảng đã đổ
--

--
-- Chỉ mục cho bảng `db_category`
--
ALTER TABLE `db_category`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `db_config`
--
ALTER TABLE `db_config`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `db_contact`
--
ALTER TABLE `db_contact`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `db_content`
--
ALTER TABLE `db_content`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `db_customer`
--
ALTER TABLE `db_customer`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `db_discount`
--
ALTER TABLE `db_discount`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `db_district`
--
ALTER TABLE `db_district`
  ADD PRIMARY KEY (`id`),
  ADD KEY `matp` (`provinceid`);

--
-- Chỉ mục cho bảng `db_order`
--
ALTER TABLE `db_order`
  ADD PRIMARY KEY (`id`),
  ADD KEY `customerid` (`customerid`),
  ADD KEY `province` (`province`),
  ADD KEY `district` (`district`),
  ADD KEY `province_2` (`province`),
  ADD KEY `district_2` (`district`),
  ADD KEY `province_3` (`province`),
  ADD KEY `district_3` (`district`);

--
-- Chỉ mục cho bảng `db_orderdetail`
--
ALTER TABLE `db_orderdetail`
  ADD PRIMARY KEY (`id`),
  ADD KEY `productid` (`productid`),
  ADD KEY `orderid` (`orderid`);

--
-- Chỉ mục cho bảng `db_producer`
--
ALTER TABLE `db_producer`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `db_product`
--
ALTER TABLE `db_product`
  ADD PRIMARY KEY (`id`),
  ADD KEY `producer` (`producer`),
  ADD KEY `catid` (`catid`);

--
-- Chỉ mục cho bảng `db_province`
--
ALTER TABLE `db_province`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `db_slider`
--
ALTER TABLE `db_slider`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `db_user`
--
ALTER TABLE `db_user`
  ADD PRIMARY KEY (`id`),
  ADD KEY `role` (`role`);

--
-- Chỉ mục cho bảng `db_usergroup`
--
ALTER TABLE `db_usergroup`
  ADD PRIMARY KEY (`id`);

--
-- AUTO_INCREMENT cho các bảng đã đổ
--

--
-- AUTO_INCREMENT cho bảng `db_category`
--
ALTER TABLE `db_category`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=14;

--
-- AUTO_INCREMENT cho bảng `db_config`
--
ALTER TABLE `db_config`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT cho bảng `db_contact`
--
ALTER TABLE `db_contact`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=11;

--
-- AUTO_INCREMENT cho bảng `db_content`
--
ALTER TABLE `db_content`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=8;

--
-- AUTO_INCREMENT cho bảng `db_customer`
--
ALTER TABLE `db_customer`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=75;

--
-- AUTO_INCREMENT cho bảng `db_discount`
--
ALTER TABLE `db_discount`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID', AUTO_INCREMENT=22;

--
-- AUTO_INCREMENT cho bảng `db_order`
--
ALTER TABLE `db_order`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=109;

--
-- AUTO_INCREMENT cho bảng `db_orderdetail`
--
ALTER TABLE `db_orderdetail`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=133;

--
-- AUTO_INCREMENT cho bảng `db_producer`
--
ALTER TABLE `db_producer`
  MODIFY `id` int(10) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=7;

--
-- AUTO_INCREMENT cho bảng `db_product`
--
ALTER TABLE `db_product`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=32;

--
-- AUTO_INCREMENT cho bảng `db_slider`
--
ALTER TABLE `db_slider`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=11;

--
-- AUTO_INCREMENT cho bảng `db_user`
--
ALTER TABLE `db_user`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=10;

--
-- AUTO_INCREMENT cho bảng `db_usergroup`
--
ALTER TABLE `db_usergroup`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;

--
-- Các ràng buộc cho các bảng đã đổ
--

--
-- Các ràng buộc cho bảng `db_district`
--
ALTER TABLE `db_district`
  ADD CONSTRAINT `db_district_ibfk_1` FOREIGN KEY (`provinceid`) REFERENCES `db_province` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Các ràng buộc cho bảng `db_order`
--
ALTER TABLE `db_order`
  ADD CONSTRAINT `db_order_ibfk_2` FOREIGN KEY (`province`) REFERENCES `db_province` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `db_order_ibfk_3` FOREIGN KEY (`district`) REFERENCES `db_district` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `db_order_ibfk_4` FOREIGN KEY (`customerid`) REFERENCES `db_customer` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Các ràng buộc cho bảng `db_orderdetail`
--
ALTER TABLE `db_orderdetail`
  ADD CONSTRAINT `db_orderdetail_ibfk_2` FOREIGN KEY (`productid`) REFERENCES `db_product` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `db_orderdetail_ibfk_3` FOREIGN KEY (`orderid`) REFERENCES `db_order` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Các ràng buộc cho bảng `db_product`
--
ALTER TABLE `db_product`
  ADD CONSTRAINT `db_product_ibfk_1` FOREIGN KEY (`catid`) REFERENCES `db_category` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `db_product_ibfk_2` FOREIGN KEY (`producer`) REFERENCES `db_producer` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION;

--
-- Các ràng buộc cho bảng `db_user`
--
ALTER TABLE `db_user`
  ADD CONSTRAINT `db_user_ibfk_1` FOREIGN KEY (`role`) REFERENCES `db_usergroup` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
